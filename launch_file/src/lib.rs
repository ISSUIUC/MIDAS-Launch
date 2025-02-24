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
use dataframe::{Data, DataFrame, DataFrameBuilder, DataFrameView, DataType};

use crate::deserialize::{SerializedCpp, Deserializer, DeserializerBuilder};

const MAIN_SRC: &'static [u8] = include_bytes!("../src-py/__main__.py");
const PARSER_SRC: &'static [u8] = include_bytes!("../src-py/cpp_parser.py");

macro_rules! try_catch {
    ($b:block) => { (|| -> Result<_, _> { $b })() };
}

pub struct Checksum(pub Result<u32, Arc<LogFormat>>);

impl Checksum {
    pub const SENTINEL: u32 = u32::from_le_bytes([0xDE, 0xAD, 0xBE, 0xEF]);
}

static SCRIPT_DIR: LazyLock<Option<ProjectDirs>> = LazyLock::new(|| {
    ProjectDirs::from("", "", "MIDAS Launch")
});


#[derive(Deserialize, Clone, Eq, PartialEq)]
pub struct LogFormat {
    #[serde(rename = "<checksum>")]
    pub checksum: u32,
    #[serde(flatten)]
    pub variants: IndexMap<String, (u32, SerializedCpp)>,
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
            checksum: Checksum::SENTINEL,
            variants
        })
    }

    pub fn from_format_file(format_file_name: &Path, python: impl AsRef<OsStr>) -> Result<Self, String> {
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
        let format = serde_json::from_str::<LogFormat>(&format).map_err(|e| format!("Could not read schema {}", e))?;

        Ok(format)
    }

    pub fn reader(&self, total_file_size: Option<u64>) -> LaunchFileReader {
        LaunchFileReader::new(self, total_file_size)
    }
}


pub struct LaunchFileReader<'f> {
    #[allow(dead_code)]
    format: &'f LogFormat,
    dataframe: DataFrame,
    row_numbers: Vec<usize>,
    file_number: i32,
    #[allow(dead_code)]
    smallest: usize,
    largest: usize,
    variants: AHashMap<u32, (NonZeroU32, Deserializer)>
}


impl<'f> LaunchFileReader<'f> {
    fn new(format: &'f LogFormat, total_file_size: Option<u64>) -> Self {
        let mut dataframe_builder = DataFrameBuilder::new();
        dataframe_builder.add_column("sensor", DataType::Intern);
        dataframe_builder.add_column("file number", DataType::Integer);
        dataframe_builder.add_column("timestamp", DataType::Integer);

        let mut variants: AHashMap<u32, (NonZeroU32, Deserializer)> = AHashMap::new();
        let mut smallest = usize::MAX;
        let mut largest = usize::MIN;
        for (name, (disc, format)) in &format.variants {
            let mut builder = DeserializerBuilder::new(name.clone(), &mut dataframe_builder);
            format.to_fast(&mut builder, name);
            let fast_format = builder.finish();
            smallest = smallest.min(fast_format.size).max(1);
            largest = largest.max(fast_format.size);

            dbg!(name, fast_format.size);
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
            format,
            dataframe,
            row_numbers,
            file_number: 0,
            smallest,
            largest,
            variants
        }
    }

    pub fn read_file(&mut self, file: &mut (impl Read + Seek), mut on_row_callback: impl FnMut(u64)) -> io::Result<u64> {
        // todo
        // if let Some(file_size) = file_size {
        //     let maximum_needed_rows = file_size.div_ceil(self.smallest as u64 + 8);
        //     self.dataframe.hint_rows()
        // }

        let mut offset: u64 = 0;
        let mut added_rows = 0;
        self.file_number += 1;

        let _checksum = file.read_u32::<LittleEndian>()?; offset += 4;

        let result: io::Result<()> = try_catch!({
            let mut read_buf = vec![0u8; self.largest].into_boxed_slice();
            let mut last: Option<&Deserializer> = None;
            let mut last_timestamp = 0;
            let mut synchronizing_amount = 0;
            loop {
                let determinant = file.read_u32::<LittleEndian>()?; offset += 4;
                let timestamp_ms = file.read_u32::<LittleEndian>()?; offset += 4;

                let Some((key, fast_format)) = self.variants.get(&determinant) else {
                    file.seek_relative(-7)?;
                    offset -= 7;
                    synchronizing_amount += 1;
                    // return Err(io::Error::other(format!("No variant for discriminant {} at offset {}", determinant, offset - 4 + rewind_amount as u64)));
                    continue;
                };
                if last_timestamp != 0 && timestamp_ms.abs_diff(last_timestamp) >= 500 {
                    file.seek_relative(-7)?;
                    offset -= 7;
                    synchronizing_amount += 1;
                    continue;
                }
                if synchronizing_amount != 0 {
                    eprintln!("Stepped {} bytes forward from offset {} to synchronize to timestamp {}.", synchronizing_amount, offset -7 - synchronizing_amount, timestamp_ms);
                    synchronizing_amount = 0;
                }
                last = Some(fast_format);
                last_timestamp = timestamp_ms;

                let row_idx = self.dataframe.add_null_row();
                let mut row = self.dataframe.row_mut(row_idx);

                row.set_col_raw(0, Some(*key));
                row.set_col_with_ty(1, DataType::Integer, Data::Integer(self.file_number - 1));
                row.set_col_with_ty(2, DataType::Integer, Data::Integer(timestamp_ms as i32));

                file.read_exact(&mut read_buf[..fast_format.size])?;

                fast_format.parse(&read_buf[..fast_format.size], &mut row);
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
        DataFrameView {
            rows: self.row_numbers,
            df: Arc::new(self.dataframe)
        }
    }
}
