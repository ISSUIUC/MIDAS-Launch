use std::io::Read;
use dataframe::{Data, DataFrameBuilder, DataType, RowMut};
use std::io;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Deserialize;
use indexmap::IndexMap;
use std::collections::HashMap;

#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum SerializedCpp {
    #[serde(rename = "bool")]
    Boolean,
    #[serde(rename = "int")]
    Integer {
        signed: bool,
        size: u8,
    },
    #[serde(rename = "float")]
    Float {
        size: u8
    },
    #[serde(rename = "enum")]
    Enum {
        variants: IndexMap<String, u32>
    },
    #[serde(rename = "array")]
    Array {
        item: Box<SerializedCpp>,
        count: u32
    },
    #[serde(rename = "struct")]
    Struct {
        members: IndexMap<String, SerializedCpp>
    },
    #[serde(rename = "union")]
    Union {
        variants: Vec<(String, SerializedCpp)>
    }
}

pub enum ReadType {
    Bool,
    I8,
    // I16,
    I32,
    // I64,
    U8,
    // U16,
    U32,
    // U64,
    F32,
    F64,
    Discriminant(u8),
    Padding(u8)
}

pub struct Deserializer {
    items: Vec<(ReadType, usize)>,
    enums: Vec<HashMap<u32, String>>,
    pub size: usize
}

impl Deserializer {
    pub fn parse<'a, 'b>(&'a self, file: &mut impl Read, row: &mut RowMut<'b>) -> io::Result<()> where 'a: 'b {
        let mut padding_buf = [0; 256];
        for (ty, offset) in &self.items {
            let offset = *offset;
            match ty {
                ReadType::Bool => {
                    row.set_col(offset, Data::Integer((file.read_u8()? != 0) as i32));
                }
                ReadType::I8 => {
                    row.set_col(offset, Data::Integer(file.read_i8()? as i32));
                }
                ReadType::I32 => {
                    row.set_col(offset, Data::Integer(file.read_i32::<LittleEndian>()?));
                }
                ReadType::U8 => {
                    row.set_col(offset, Data::Integer(file.read_u8()? as i32));
                }
                ReadType::U32 => {
                    row.set_col(offset, Data::Integer(file.read_u32::<LittleEndian>()? as i32));
                }
                ReadType::F32 => {
                    row.set_col(offset, Data::Float(file.read_f32::<LittleEndian>()?));
                }
                ReadType::F64 => {
                    row.set_col(offset, Data::Float(file.read_f64::<LittleEndian>()? as f32));
                }
                ReadType::Discriminant(idx) => {
                    let disc = file.read_u32::<LittleEndian>()?;
                    let name = self.enums[*idx as usize].get(&disc).map_or("<unknown>", |name| name);
                    row.set_col(offset, Data::Str(name));
                }
                ReadType::Padding(amount) => {
                    file.read_exact(&mut padding_buf[..*amount as usize])?;
                }
            }
        }
        Ok(())
    }
}

pub struct DeserializerBuilder<'a> {
    builder: &'a mut DataFrameBuilder,

    items: Vec<(ReadType, usize)>,
    offset: usize,
    enums: Vec<HashMap<u32, String>>
}

impl<'a> DeserializerBuilder<'a> {
    pub fn new(builder: &'a mut DataFrameBuilder) -> DeserializerBuilder<'a> {
        DeserializerBuilder {
            builder,
            items: vec![],
            offset: 0,
            enums: vec![]
        }
    }

    pub fn finish(self) -> Deserializer {
        Deserializer { items: self.items, enums: self.enums, size: self.offset }
    }

    fn read_bool(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Integer);
        self.items.push((ReadType::Bool, offset));
        self.offset += 1;
    }

    fn read_i8(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Integer);
        self.items.push((ReadType::I8, offset));
        self.offset += 1;
    }

    fn read_i32(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Integer);
        self.items.push((ReadType::I32, offset));
        self.offset += 4;
    }

    fn read_u8(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Integer);
        self.items.push((ReadType::U8, offset));
        self.offset += 1;
    }

    fn read_u32(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Integer);
        self.items.push((ReadType::U32, offset));
        self.offset += 4;
    }

    fn read_f32(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Float);
        self.items.push((ReadType::F32, offset));
        self.offset += 4;
    }

    fn read_f64(&mut self, name: impl Into<String>) {
        let offset = self.builder.add_column(name, DataType::Float);
        self.items.push((ReadType::F64, offset));
        self.offset += 8;
    }

    fn read_enum(&mut self, name: impl Into<String>, variants: HashMap<u32, String>) {
        let offset = self.builder.add_column(name, DataType::Intern);
        let idx = self.enums.len() as u8;
        self.enums.push(variants);
        self.items.push((ReadType::Discriminant(idx), offset));
        self.offset += 4;
    }

    fn align_to(&mut self, align: u8) {
        let amount = self.offset.next_multiple_of(align as usize) - self.offset;
        if amount != 0 {
            self.items.push((ReadType::Padding(amount as u8), 0));
            self.offset += amount;
        }
    }
}

impl SerializedCpp {
    fn align(&self) -> u8 {
        match self {
            SerializedCpp::Boolean => 1,
            SerializedCpp::Integer { size, .. } => *size,
            SerializedCpp::Float { size, .. } => *size,
            SerializedCpp::Enum { .. } => 4,
            SerializedCpp::Array { item, .. } => item.align(),
            SerializedCpp::Struct { members } => members.values().map(|ty| ty.align()).max().unwrap_or(1),
            SerializedCpp::Union { .. } => todo!(),
        }
    }

    pub fn to_fast(&self, file: &mut DeserializerBuilder, name: &str) -> u8 {
        let value = match self {
            SerializedCpp::Boolean => {
                file.read_bool(name);
                1
            }
            SerializedCpp::Integer { signed: true, size } => {
                if *size == 1 {
                    file.read_i8(name);
                    1
                } else if *size == 4 {
                    file.read_i32(name);
                    4
                } else {
                    panic!("{}", *size);
                }
            }
            SerializedCpp::Integer { signed: false, size } => {
                if *size == 1 {
                    file.read_u8(name);
                    1
                } else if *size == 4 {
                    file.read_u32(name);
                    4
                } else {
                    panic!("{}", *size);
                }
            }
            SerializedCpp::Float { size } => {
                if *size == 4 {
                    file.read_f32(name);
                    4
                } else if *size == 8 {
                    file.read_f64(name);
                    8
                } else {
                    panic!("{}", *size);
                }
            }
            SerializedCpp::Enum { variants } => {
                let mut new_variants = HashMap::new();
                for (name, disc) in variants {
                    new_variants.insert(*disc, name.clone());
                }
                file.read_enum(name, new_variants);
                4
            }
            SerializedCpp::Array { item, count } => {
                let mut align = 1;
                for i in 0..*count {
                    align = item.to_fast(file, &format!("{}[{}]", name, i));
                    file.align_to(align);
                }
                align
            }
            SerializedCpp::Struct { members } => {
                let mut max_align = 1;
                for (field_name, format) in members {
                    file.align_to(format.align());

                    let align = format.to_fast(file, &format!("{}.{}", name, field_name));
                    if align > max_align {
                        max_align = align;
                    }
                }
                file.align_to(max_align);
                max_align
            }
            SerializedCpp::Union { .. } => {
                todo!()
            }
        };
        value
    }
}
