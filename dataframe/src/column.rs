use std::cmp::Ordering;
use crate::data::{Data, DataType};

pub trait Column {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn data_type(&self) -> DataType;

    fn get_row_data(&self, index: usize) -> Data;

    fn compare(&self, a: usize, b: usize) -> Ordering;
}

pub(crate) trait ColumnInternal: Column {
    fn underlying_rows(&self) -> usize;
}

pub trait ColumnMut: Column {
    fn set_row_data(&mut self, index: usize, data: &Data);
}

pub(crate) trait ColumnMutInternal: ColumnMut + ColumnInternal {
    fn hint_complete(&mut self);
    fn push_data(&mut self, item: &Data);
}