mod deserialize;

use std::sync::Arc;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::{fs, fs::File};
use std::{io, io::{Read, Write}};
use std::path::Path;
use std::process::Command;
use ahash::AHashMap;

use indexmap::IndexMap;
use serde::Deserialize;
use byteorder::{LittleEndian, ReadBytesExt};
use directories::ProjectDirs;
use dataframe::{Data, DataFrameBuilder, DataFrameView, DataType};

use deserialize::SerializedCpp;
use crate::deserialize::{Deserializer, DeserializerBuilder};

const MAIN_SRC: &'static [u8] = include_bytes!("../src-py/__main__.py");
const PARSER_SRC: &'static [u8] = include_bytes!("../src-py/cpp_parser.py");

macro_rules! try_catch {
    ($b:block) => { (|| -> Result<_, _> { $b })() };
}


#[derive(Deserialize, Clone)]
pub struct LogFormat {
    #[serde(rename = "<checksum>")]
    pub checksum: u32,
    #[serde(flatten)]
    pub variants: IndexMap<String, (u32, SerializedCpp)>,
}

impl LogFormat {
    pub fn clear_scripts() {
        let script_dir = ProjectDirs::from("", "", "MIDAS Launch").unwrap();
        fs::create_dir_all(script_dir.data_dir()).unwrap();
        let _ = fs::remove_file(script_dir.data_dir().join("__main__.py"));
        let _ = fs::remove_file(script_dir.data_dir().join("cpp_parser.py"));
    }

    pub fn from_file(format_file_name: &Path, python: impl AsRef<OsStr>) -> Result<Self, String> {
        let script_dir = ProjectDirs::from("", "", "MIDAS-Launch")
            .ok_or("Could not find script.".to_string())?;

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

    pub fn read_file(&self, file: &mut impl Read, file_size: Option<u64>, mut on_row_callback: impl FnMut(u64)) -> io::Result<DataFrameView> {
        let mut dataframe_builder = DataFrameBuilder::new();
        dataframe_builder.add_column("sensor", DataType::Intern);
        dataframe_builder.add_column("timestamp", DataType::Integer);

        let mut variants: AHashMap<u32, (String, Deserializer)> = AHashMap::new();
        let mut smallest = usize::MAX;
        for (name, (disc, format)) in &self.variants {
            let mut builder = DeserializerBuilder::new(&mut dataframe_builder);
            format.to_fast(&mut builder, name);
            let fast_format = builder.finish();
            smallest = smallest.min(fast_format.size).max(1);
            variants.insert(*disc, (name.clone(), fast_format));
        }
        let mut dataframe;
        let mut row_numbers = Vec::new();
        if let Some(file_size) = file_size {
            let rows = (file_size / (smallest as u64 + 8)) as usize;
            dataframe = dataframe_builder.build_with_capacity(rows);
            row_numbers.reserve(rows);
        } else {
            dataframe = dataframe_builder.build();
        }

        let mut offset: u64 = 0;
        let mut i = 0;

        let _checksum = file.read_u32::<LittleEndian>()?; offset += 4;

        let result: io::Result<()> = try_catch!({
            loop {
                let row_idx = dataframe.add_null_row();
                let mut row = dataframe.row_mut(row_idx);

                let determinant = file.read_u32::<LittleEndian>()?; offset += 4;
                let timestamp_ms = file.read_u32::<LittleEndian>()?; offset += 4;

                let (name, fast_format) = variants.get(&determinant)
                    .ok_or_else(|| io::Error::other(format!("No variant for discriminant {} at offset {}", determinant, offset - 8)))?;

                row.set_col(0, Data::Str(name));
                row.set_col(1, Data::Integer(timestamp_ms as i32));

                fast_format.parse(file, &mut row)?;
                row_numbers.push(i);
                offset += fast_format.size as u64;
                i += 1;

                on_row_callback(offset);
            }
        });
        let result = result.unwrap_err();

        dataframe.hint_complete();

        if result.kind() == io::ErrorKind::UnexpectedEof {
            Ok(DataFrameView {
                rows: row_numbers,
                df: Arc::new(dataframe)
            })
        } else {
            Err(result)
        }
    }
}
