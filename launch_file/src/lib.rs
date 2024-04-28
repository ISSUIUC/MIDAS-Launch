mod deserialize;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::{fs, fs::File, io};
use std::env::var;
use std::io::{Read, Write};
use std::mem::transmute;
use std::path::Path;
use std::process::Command;

use indexmap::IndexMap;
use serde::Deserialize;
use byteorder::{ReadBytesExt, LittleEndian};
use directories::ProjectDirs;
use dataframe::{Data, DataFrame, DataFrameBuilder, DataType, DataTypeNew};

use deserialize::SerializedCpp;
use crate::deserialize::{Deserializer, DeserializerBuilder, ReadType};

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

    fn convert_to_datatype_new(ty: &ReadType) -> DataTypeNew {
        match ty {
            ReadType::Bool => DataTypeNew::Bool,
            ReadType::I8 => DataTypeNew::I8,
            ReadType::I32 => DataTypeNew::I32,
            ReadType::U8 => DataTypeNew::U8,
            ReadType::U32 => DataTypeNew::U32,
            ReadType::F32 => DataTypeNew::F32,
            ReadType::F64 => DataTypeNew::F64,
            ReadType::Discriminant(_) => DataTypeNew::Enum,
            ReadType::Padding(_) => panic!()
        }
    }

    pub fn read_file(&self, file: &mut impl Read, mut on_row_callback: impl FnMut(u64)) -> io::Result<DataFrame> {
        let mut dataframebuilder = DataFrameBuilder::new();
        dataframebuilder.add_col("sensor", DataTypeNew::Enum,0);
        dataframebuilder.add_col("timestamp", DataTypeNew::U32,0);

        for (name, (disc, format)) in &self.variants {
            let mut builder = DeserializerBuilder::new(&mut dataframebuilder, *disc as usize);
            format.to_fast(&mut builder, name);
        }

        let mut dataframe = dataframebuilder.build();

        let mut offset: u64 = 0;

        let _checksum = file.read_u32::<LittleEndian>()?; offset += 4;

        let result: io::Result<()> = try_catch!({
            loop {
                dataframe.add_row();
                let mut det_time = [0;8];
                file.read_exact(&mut det_time)?; offset += 8;

                let [determinant, timestamp_ms]: [u32;2] = unsafe { transmute(det_time) };

                dataframe.get_slice_for(0).copy_from_slice(&det_time);
                let dest_slice = dataframe.get_slice_for(determinant as usize);
                file.read_exact(dest_slice)?;

                offset += dest_slice.len() as u64;
                on_row_callback(offset);
            }
        });
        let result = result.unwrap_err();

        if result.kind() == io::ErrorKind::UnexpectedEof {
            Ok(dataframe)
        } else {
            Err(result)
        }
    }
}
