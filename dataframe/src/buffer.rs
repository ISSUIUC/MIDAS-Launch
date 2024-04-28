use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use smallvec::{SmallVec, smallvec};
use crate::data::DataTypeNew;

use super::Shape;
use super::data::{ColumnData, Data, DataType, Enum, Float, Integer};

pub type DataFrame = DataFrameNew;
// type DataFrame = DataFrameOld;

const ROWS_PER_BLOCK: usize = 1<<20;

#[derive(Clone)]
enum ColumnDescriptionWithPad {
    Desc(ColumnDescription),
    Pad(usize),
}
#[derive(Clone)]
struct ColumnDescription {
    ty: DataTypeNew,
    idx: usize,
    name: String,
}

#[derive(Clone, Debug)]
struct ColumnAlignment {
    ty: DataTypeNew,
    idx: usize,
    name: String,
    offset: usize,
}

#[derive(Clone, Debug)]
struct PacketAlignment {
    offset: usize,
    width: usize,
}

pub struct DataFrameBuilder {
    column_groups: Vec<Vec<ColumnDescriptionWithPad>>,
    column_count: usize,
}

impl DataFrameBuilder {
    pub fn new() -> Self {
        Self {
            column_groups: Vec::new(),
            column_count: 0
        }
    }

    pub fn add_col(&mut self, name: impl Into<String>, ty: DataTypeNew, packet_id: usize) -> usize {
        while self.column_groups.len() <= packet_id {
            self.column_groups.push(vec![]);
        }

        let idx = self.column_count;

        self.column_groups[packet_id].push(ColumnDescriptionWithPad::Desc(ColumnDescription{
            ty,
            idx,
            name: name.into(),
        }));

        self.column_count += 1;

        idx
    }

    pub fn add_pad(&mut self, amount: usize, packet_id: usize) {
        while self.column_groups.len() <= packet_id {
            self.column_groups.push(vec![]);
        }

        self.column_groups[packet_id].push(ColumnDescriptionWithPad::Pad(amount));
    }

    pub fn build(&self) -> DataFrameNew {
        let mut total_offset = 0;
        let mut aligned_cols = vec![];
        let mut packets = vec![];
        for cols in &self.column_groups {
            let mut local_offset = 0;
            for c in cols {
                match c {
                    ColumnDescriptionWithPad::Desc(c) => {
                        aligned_cols.push(ColumnAlignment{
                            ty: c.ty,
                            idx: c.idx,
                            name: c.name.clone(),
                            offset: total_offset + local_offset
                        });
                        local_offset += c.ty.width()
                    },
                    ColumnDescriptionWithPad::Pad(pad) => {
                        local_offset += pad
                    }
                }

            }
            packets.push(PacketAlignment{
                offset: total_offset,
                width: local_offset
            });
            total_offset += local_offset
        }

        DataFrameNew::new(&packets, &aligned_cols, total_offset)
    }
}

#[derive(Clone)]
pub struct DataFrameNew {
    backing: Vec<Vec<u8>>,
    width: usize,
    rows: usize,
    columns: Vec<ColumnAlignment>,
    packets: Vec<PacketAlignment>,
}

impl DataFrameNew {
    pub fn new(packets: &[PacketAlignment], columns: &[ColumnAlignment], row_width: usize) -> Self {
        Self {
            backing: vec![],
            width: row_width,
            rows: 0,
            columns: columns.to_vec(),
            packets: packets.to_vec(),
        }
    }

    pub fn column_names(&self) -> impl Iterator<Item=&str> {
        self.columns.iter().map(|c|c.name.as_str())
    }

    pub fn column_name(&self, col: usize) -> &str {
        &self.columns[col].name
    }

    pub fn column_type(&self, col: usize) -> DataType {
        self.columns[col].ty.data_type()
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        (0..self.columns.len()).map(move |i|self.get_data(index,i))
    }

    pub fn add_row(&mut self) {
        if self.rows % ROWS_PER_BLOCK == 0 {
            let mut block = vec![];
            block.resize(self.width * ROWS_PER_BLOCK, 0xff);
            self.backing.push(block);
        }
        self.rows += 1;
    }

    pub fn get_slice_for(&mut self, packet_id: usize) -> &mut [u8] {
        let p = &self.packets[packet_id];

        let block_idx = (self.rows - 1) / ROWS_PER_BLOCK;
        let block_offset = ((self.rows - 1) % ROWS_PER_BLOCK) * self.width + p.offset;
        &mut self.backing[block_idx][block_offset..(block_offset+p.width)]
    }

    pub fn shape(&self) -> Shape {
        Shape { rows: self.rows, cols: self.columns.len() }
    }

    pub fn get_data(&self, row: usize, col: usize) -> Data {
        let block_idx = row / ROWS_PER_BLOCK;
        let row_offset = (row % ROWS_PER_BLOCK) * self.width;
        let col = &self.columns[col];
        let base_offset = col.offset;
        let width = col.ty.width();

        let off = row_offset + base_offset;
        let slice = &self.backing[block_idx][off..(off+width)];
        if slice.iter().all(|x|*x == 0xff) {
            Data::Null
        } else {
            col.ty.read(slice)
        }
    }

    pub fn set_data(&mut self, row: usize, col: usize, data: &Data) {
        todo!()
    }
}

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
pub struct DataFrameOld {
    columns: Vec<ColumnDesc>,
    items: Vec<SmallVec<[DataItem;5]>>,
    enum_map: Vec<String>
}

impl DataFrameOld {
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