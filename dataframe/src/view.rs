use std::cmp::Ordering;
use std::sync::Arc;

use crate::data::{Data, DataType};
use crate::{DataFrame, Shape};


#[derive(Clone)]
pub struct DataFrameView {
    pub rows: Vec<usize>,
    pub df: Arc<DataFrame>
}

impl DataFrameView {
    pub fn from_dataframe(df: DataFrame) -> DataFrameView {
        DataFrameView {
            rows: (0..df.shape().rows).collect(),
            df: Arc::new(df)
        }
    }

    pub fn shape(&self) -> Shape {
        Shape {
            rows: self.rows.len(),
            cols: self.df.shape().cols
        }
    }

    pub fn col_names(&self) -> impl Iterator<Item=&str> {
        self.df.column_names()
    }

    pub fn col_name(&self, idx: usize) -> &str {
        self.df.column_name(idx)
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        self.df.row_iter(self.rows[index])
    }

    pub fn get_by_index(&self, col: usize, row: usize) -> Data {
        self.df.get_data(self.rows[row],col)
    }

    pub fn set_by_index(&mut self, col: usize, row: usize, data: &Data) {
        Arc::make_mut(&mut self.df).set_data(self.rows[row],col, data)
    }

    pub fn filter_by(&mut self, col: usize, mut f: impl FnMut(usize, &Data) -> bool) {
        let mut indices = vec![];

        for row_idx in 0..self.rows.len() {
            let data = self.get_by_index(col, row_idx);
            if f(row_idx, &data) {
                indices.push(self.rows[row_idx]);
            }
        }
        self.rows = indices;
    }

    pub fn sort_by_asc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        rows_sorted.sort_by(|a_idx, b_idx| self.get_by_index(col, *a_idx).compare(&self.get_by_index(col, *b_idx)).expect("bad data type"));
        self.rows = rows_sorted;
    }

    pub fn sort_by_desc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        rows_sorted.sort_by(|a_idx, b_idx| self.get_by_index(col, *a_idx).compare(&self.get_by_index(col, *b_idx)).expect("bad data type"));
        self.rows = rows_sorted;
    }
}
