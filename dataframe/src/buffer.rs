use std::cmp::Ordering;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use smallvec::{SmallVec, smallvec};

use super::Shape;
use super::data::{ColumnData, Data, DataType, Enum, Float, Integer};


#[derive(Clone)]
enum DataUnion {
    Float(f64),
    Integer(i64),
    StrIdx(u64),
    Null
}

#[derive(Clone)]
struct DataItem {
    which: u8,
    val: DataUnion
}

#[derive(Clone)]
pub struct DataFrame {
    columns: Vec<ColumnDesc>,
    items: Vec<SmallVec<[DataItem;5]>>,
    enum_map: Vec<String>
}

impl DataFrame {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            items: vec![],
            enum_map: vec![],
        }
    }

    pub fn shape(&self) -> Shape {
        Shape { rows: self.items.len(), cols: self.columns.len() }
    }

    pub fn hint_complete(&mut self) {}

    pub fn add_null_col(&mut self, name: impl Into<String>, ty: DataType) -> usize {
        let new_idx = self.columns.len();
        assert!(new_idx < u8::MAX as usize);
        self.columns.push(ColumnDesc {name: name.into(),ty});

        new_idx
    }

    pub fn add_blank_row(&mut self) {
        self.items.push(smallvec![]);
    }

    pub unsafe fn append_item_unchecked(&mut self, col: usize, data: Data) {
        let dataitem = self.to_data_item(&data, col);
        self.items.last_mut().unwrap().push(dataitem);
    }

    pub fn add_row(&mut self, row: &[Data]) {
        let row = row.iter().enumerate()
            .filter(|(i,x)|!x.is_null())
            .map(|(i,x)|self.to_data_item(x, i))
            .collect();
        self.items.push(row);
    }

    pub fn column_names(&self) -> impl Iterator<Item=&str> {
        self.columns.iter().map(|c|c.name.as_str())
    }

    pub fn column_name(&self, col: usize) -> &str {
        &self.columns[col].name
    }

    pub fn column_type(&self, col: usize) -> DataType {
        self.columns[col].ty
    }

    fn to_data_item(&mut self, data: &Data, column: usize) -> DataItem {
        let val = match data {
            Data::Integer(i) => DataUnion::Integer(*i),
            Data::Str(s) => DataUnion::StrIdx(self.get_or_add_enum_idx(s)),
            Data::Float(f) => DataUnion::Float(*f),
            Data::Null => DataUnion::Null,
        };

        DataItem {
            which: column as u8,
            val
        }
    }

    fn from_data_item(&self, data: &DataItem) -> Data {
        match data.val {
            DataUnion::Float(f) => Data::Float(f),
            DataUnion::Integer(i) => Data::Integer(i),
            DataUnion::StrIdx(s) => Data::Str(self.get_enum_str(s)),
            DataUnion::Null => Data::Null,
        }
    }

    fn get_or_add_enum_idx(&mut self, enum_str: &str) -> u64 {
        if let Some(found) = self.enum_map.iter().enumerate().find(|(i,x)|*x == enum_str).map(|(i,_)|i) {
            found as u64
        } else {
            self.enum_map.push(enum_str.into());
            (self.enum_map.len() - 1) as u64
        }
    }

    fn get_enum_str(&self, enum_idx: u64) -> &str{
        &self.enum_map[enum_idx as usize]
    }

    pub fn get_data(&self, row: usize, col: usize) -> Data {
        if let Some(data) = self.items[row].iter().find(|x|x.which as usize == col) {
            self.from_data_item(data)
        } else {
            Data::Null
        }
    }

    pub fn set_data(&mut self, row: usize, col: usize, data: &Data) {
        let data_item = self.to_data_item(data, col);

        if let Some(data) = self.items[row].iter_mut().find(|x|x.which as usize == col) {
            *data = data_item
        } else {
            self.items[row].push(data_item);
        }
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        let row = &self.items[index];
        self.columns.iter().enumerate().map(|(i,c)|{
            if let Some(item) = row.iter().find(|x|x.which as usize == i) {
                self.from_data_item(item)
            } else {
                Data::Null
            }
        })
    }
}

#[derive(Clone)]
struct ColumnDesc {
    name: String,
    ty: DataType,
}

impl ColumnDesc {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.ty
    }
}