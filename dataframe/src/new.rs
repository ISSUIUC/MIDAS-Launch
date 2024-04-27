use std::iter;
use std::ops::{Index, Range};
use string_interner::backend::StringBackend;
use string_interner::StringInterner;
use string_interner::symbol::SymbolU32;
use crate::{Data, data, data::DataType};
use crate::data::ColumnData;

struct ColumnInfo {
    offset: usize,
    name: String,
    ty: DataType,
}

struct ColumnLayout {
    null_row: Box<[u8]>,
    interner: StringInterner<StringBackend>,
    columns: Vec<ColumnInfo>,
}

struct Row<'df> {
    mem: *const u8,
    layout: &'df ColumnLayout
}

impl<'df> Row<'df> {
    fn get_col(&self, idx: usize) -> Data {
        let col_info = &self.layout.columns[idx];
        match col_info.ty {
            DataType::Integer => {
                let value = unsafe {
                    self.mem.wrapping_byte_add(col_info.offset).cast::<data::Integer>().read()
                };
                value.to_data(&())
            }
            DataType::Float => {
                let value = unsafe {
                    self.mem.wrapping_byte_add(col_info.offset).cast::<data::Float>().read()
                };
                value.to_data(&())
            }
            DataType::Enum => {
                let value = unsafe {
                    self.mem.wrapping_byte_add(col_info.offset).cast::<data::Enum>().read()
                };
                value.to_data(&self.layout.interner)
            }
        }
    }
}

struct Dataframe {
    mem: Vec<u8>,
    rows: usize,
    layout: ColumnLayout
}

impl Dataframe {
    pub fn row(&self, index: usize) -> Row<'_> {
        debug_assert!(index < self.rows);
        Row {
            mem: self.mem.as_ptr().wrapping_byte_add(self.layout.null_row.len() * index),
            layout: &self.layout
        }
    }

    pub fn add_null_row(&mut self) {
        self.mem.extend(&self.layout.null_row);
        self.rows += 1;
    }
}