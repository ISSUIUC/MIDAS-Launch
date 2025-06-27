mod deserialize;
mod bytes;

use std::sync::{Arc, LazyLock};
use std::ffi::OsStr;
use std::{fs, fs::File};
use std::{io, io::{Read, Write}};
use std::io::Seek;
use std::num::NonZeroU32;
use std::path::Path;
use std::process::Command;

use ahash::AHashMap;
use indexmap::IndexMap;
use serde::Deserialize;
use byteorder::{LittleEndian, ReadBytesExt};
use directories::ProjectDirs;
use dataframe::{Data, DataFrame, DataFrameView, DataType};

use crate::deserialize::{SerializedCpp, Deserializer, DeserializerBuilder};

const MAIN_SRC: &'static [u8] = include_bytes!("../src-py/__main__.py");
const PARSER_SRC: &'static [u8] = include_bytes!("../src-py/cpp_parser.py");

macro_rules! try_catch {
    ($b:block) => { (|| -> Result<_, _> { $b })() };
}

pub const SENTINEL: u32 = u32::from_le_bytes([0xDE, 0xAD, 0xBE, 0xEF]);

static SCRIPT_DIR: LazyLock<Option<ProjectDirs>> = LazyLock::new(|| {
    ProjectDirs::from("", "", "MIDAS Launch")
});


#[derive(Clone)]
pub enum FormatType {
    External { checksum: u32 },
    Inline(Arc<LogFormat>)
}

impl FormatType {
    pub fn from_file(file: &mut impl Read) -> io::Result<FormatType> {
        let mut buf = [0; 4];
        file.read_exact(&mut buf)?;
        let checksum_raw = u32::from_le_bytes(buf);
        if checksum_raw == SENTINEL {
            let mut buf = [0; 2];
            file.read_exact(&mut buf)?;
            let length = u16::from_le_bytes(buf);
            let mut format_header = vec![0; length as usize];
            file.read_exact(&mut format_header)?;

            Ok(FormatType::Inline(Arc::new(LogFormat::from_inline_header(&format_header).map_err(io::Error::other)?)))
        } else {
            Ok(FormatType::External { checksum: checksum_raw })
        }
    }
}

#[derive(Eq, PartialEq)]
pub struct LogFormat {
    skipped_bytes: u32,
    variants: IndexMap<String, (u32, SerializedCpp)>,
}

impl LogFormat {
    pub fn clear_scripts() {
        if let Some(script_dir) = SCRIPT_DIR.as_ref() {
            fs::create_dir_all(script_dir.data_dir()).unwrap();
            let _ = fs::remove_file(script_dir.data_dir().join("__main__.py"));
            let _ = fs::remove_file(script_dir.data_dir().join("cpp_parser.py"));
        }
    }

    pub fn from_inline_header(data: &[u8]) -> Result<Self, String> {
        let variants = bytes::from_inline_header_helper(data).ok_or("Malformed Header!".to_owned())?;
        Ok(LogFormat {
            skipped_bytes: 4 + 2 + data.len() as u32,
            variants
        })
    }

    pub fn from_format_file(format_file_name: &Path, python: impl AsRef<OsStr>) -> Result<(u32, Self), String> {
        #[derive(Deserialize)]
        pub struct SerializedLogFormat {
            #[serde(rename = "<checksum>")]
            pub checksum: u32,
            #[serde(flatten)]
            pub variants: IndexMap<String, (u32, SerializedCpp)>,
        }

        let script_dir = SCRIPT_DIR.as_ref().ok_or("Could not find script.".to_string())?;

        fs::create_dir_all(script_dir.data_dir()).map_err(|e| format!("Could not create script: {}", e))?;
        fs::create_dir_all(script_dir.cache_dir()).map_err(|e| format!("Could not create script: {}", e))?;
        let main_path = script_dir.data_dir().join("__main__.py");
        let parser_path = script_dir.data_dir().join("cpp_parser.py");
        let main_res = File::create_new(&main_path);
        match main_res {
            Ok(mut file) => {
                file.write_all(MAIN_SRC).map_err(|e| format!("Could not create script: {}", e))?;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                ()
            }
            Err(e) => { return Err(format!("Could not find script: {}", e)); }
        }

        let parser_res = File::create_new(&parser_path);
        match parser_res {
            Ok(mut file) => {
                file.write_all(PARSER_SRC).map_err(|e| format!("Could not create script: {}", e))?;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                ()
            }
            Err(e) => { return Err(format!("Could not find script: {}", e)); }
        }

        let schema_path = script_dir.cache_dir().join("schema.json");

        let mut command = Command::new(python);

        command
            .arg(&main_path)
            .arg("-S")
            .arg("--format")
            .arg(&format_file_name)
            .arg("--out")
            .arg(&schema_path);
        let output = command
            .output()
            .map_err(|e| format!("Could not run python: {}", e))?;

        if !output.status.success() {
            return Err(format!("Script Error: {}", String::from_utf8_lossy(&output.stderr)));
        }

        let format = fs::read_to_string(&schema_path).map_err(|e| format!("Could not read schema {}", e))?;
        let format = serde_json::from_str::<SerializedLogFormat>(&format).map_err(|e| format!("Could not read schema {}", e))?;

        Ok((format.checksum, LogFormat {
            skipped_bytes: 4,
            variants: format.variants
        }))
    }

    pub fn reader(&self, total_file_size: Option<u64>) -> LaunchFileReader {
        LaunchFileReader::new(self, total_file_size)
    }
}


pub struct LaunchFileReader {
    dataframe: DataFrame,
    row_numbers: Vec<usize>,
    file_number: i32,
    skipped_bytes: u32,
    variants: AHashMap<u32, (NonZeroU32, Deserializer)>,
    read_buffer: Box<[u8]>
}


impl LaunchFileReader {
    fn new(format: &LogFormat, total_file_size: Option<u64>) -> Self {
        let mut dataframe_builder = DataFrame::builder();
        dataframe_builder.add_column("sensor", DataType::Intern);
        dataframe_builder.add_column("file number", DataType::Integer);
        dataframe_builder.add_column("timestamp", DataType::Integer);

        let mut variants: AHashMap<u32, (NonZeroU32, Deserializer)> = AHashMap::new();
        let mut smallest = usize::MAX;
        let mut largest = usize::MIN;
        for (name, (disc, format)) in &format.variants {
            let mut builder = DeserializerBuilder::new(&mut dataframe_builder);
            format.to_fast(&mut builder, name);
            let fast_format = builder.finish();
            smallest = smallest.min(fast_format.size).max(1);
            largest = largest.max(fast_format.size);

            let key = dataframe_builder.add_interned_string(name);
            variants.insert(*disc, (key, fast_format));
        }
        let dataframe;
        let mut row_numbers = Vec::new();

        if let Some(file_size) = total_file_size {
            let rows = file_size.div_ceil(smallest as u64 + 8) as usize;
            dataframe = dataframe_builder.build_with_capacity(rows);
            row_numbers.reserve(rows);
        } else {
            dataframe = dataframe_builder.build();
        }
        LaunchFileReader {
            dataframe,
            row_numbers,
            file_number: 0,
            skipped_bytes: format.skipped_bytes,
            variants,
            read_buffer: vec![0u8; largest].into_boxed_slice()
        }
    }

    pub fn read_file(&mut self, file: &mut (impl Read + Seek), mut on_row_callback: impl FnMut(u64)) -> io::Result<u64> {
        let mut offset: u64 = 0;
        let mut added_rows = 0;
        self.file_number += 1;

        file.seek_relative(self.skipped_bytes as i64)?; offset += self.skipped_bytes as u64;

        let result: io::Result<()> = try_catch!({
            let mut last_timestamp = 0;
            let mut synchronizing_amount = 0;
            loop {
                let determinant = file.read_u32::<LittleEndian>()?; offset += 4;
                let timestamp_ms = file.read_u32::<LittleEndian>()?; offset += 4;

                let Some((key, fast_format)) = self.variants.get(&determinant) else {
                    file.seek_relative(-7)?;
                    offset -= 7;
                    synchronizing_amount += 1;
                    continue;
                };
                if last_timestamp != 0 && timestamp_ms.abs_diff(last_timestamp) >= 500 {
                    file.seek_relative(-7)?;
                    offset -= 7;
                    synchronizing_amount += 1;
                    continue;
                }
                if synchronizing_amount != 0 {
                    eprintln!("Stepped {} bytes forward from offset {} to synchronize to timestamp {}.", synchronizing_amount, offset - 7 - synchronizing_amount, timestamp_ms);
                    synchronizing_amount = 0;
                }
                last_timestamp = timestamp_ms;

                let row_idx = self.dataframe.add_null_row();
                let mut row = self.dataframe.row_mut(row_idx);

                row.set_col_raw(0, Some(*key));
                row.set_col_with_ty(1, DataType::Integer, Data::Integer(self.file_number - 1));
                row.set_col_with_ty(2, DataType::Integer, Data::Integer(timestamp_ms as i32));

                file.read_exact(&mut self.read_buffer[..fast_format.size])?;

                fast_format.parse(&self.read_buffer[..fast_format.size], &mut row);
                self.row_numbers.push(row_idx);
                offset += fast_format.size as u64;
                added_rows += 1;

                on_row_callback(offset);
            }
        });

        let result = result.unwrap_err();
        if result.kind() == io::ErrorKind::UnexpectedEof {
            Ok(added_rows)
        } else {
            Err(result)
        }
    }

    pub fn finish(mut self) -> DataFrameView {
        self.dataframe.hint_complete();
        DataFrameView::from_dataframe_and_rows(self.dataframe, self.row_numbers)
    }
}
