use std::cmp::Ordering;
use std::sync::Arc;

use crate::data::{Data, DataType};
use crate::{DataFrame, Shape};
use crate::column::{Column, ColumnInternal, ColumnMut, ColumnMutInternal};

pub struct ColumnView<'a, C>(&'a Vec<usize>, &'a C);
pub struct ColumnViewMut<'a, C>(&'a mut Vec<usize>, &'a mut C);

impl<'a, C: Column> Column for ColumnView<'a, C> {
    fn name(&self) -> &str { self.1.name() }
    fn len(&self) -> usize { self.0.len() }
    fn data_type(&self) -> DataType { self.1.data_type() }

    fn get_row_data(&self, index: usize) -> Data {
        self.1.get_row_data(self.0[index])
    }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.1.compare(self.0[a], self.0[b])
    }
}

impl<'a, C: ColumnInternal> ColumnInternal for ColumnView<'a, C> {
    fn underlying_rows(&self) -> usize { self.1.underlying_rows() }
}

impl<'a, C: Column> Column for ColumnViewMut<'a, C> {
    fn name(&self) -> &str { self.1.name() }
    fn len(&self) -> usize { self.0.len() }
    fn data_type(&self) -> DataType { self.1.data_type() }

    fn get_row_data(&self, index: usize) -> Data {
        self.1.get_row_data(self.0[index])
    }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.1.compare(self.0[a], self.0[b])
    }
}

impl<'a, C: ColumnMut> ColumnMut for ColumnViewMut<'a, C> {
    fn set_row_data(&mut self, index: usize, data: &Data) {
        self.1.set_row_data(self.0[index], data)
    }
}

impl<'a, C: ColumnInternal> ColumnInternal for ColumnViewMut<'a, C> {
    fn underlying_rows(&self) -> usize { self.1.underlying_rows() }
}

impl<'a, C: ColumnMutInternal> ColumnMutInternal for ColumnViewMut<'a, C> {
    fn hint_complete(&mut self) { self.1.hint_complete() }

    fn push_data(&mut self, item: &Data) {
        self.0.push(self.1.underlying_rows());
        self.1.push_data(item);
    }
}



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
            cols: self.df.columns.len()
        }
    }

    pub fn col_names(&self) -> impl Iterator<Item=&str> {
        self.df.columns.iter().map(|col| col.name())
    }

    pub fn col_name(&self, idx: usize) -> &str {
        self.df.columns[idx].name()
    }

    pub fn col(&self, idx: usize) -> ColumnView<'_, impl Column> {
        ColumnView(&self.rows, &self.df.columns[idx])
    }

    pub fn col_mut(&mut self, idx: usize) -> ColumnViewMut<'_, impl ColumnMut> {
        ColumnViewMut(&mut self.rows, &mut Arc::make_mut(&mut self.df).columns[idx])
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        self.df.row_iter(self.rows[index])
    }

    pub fn get_by_index(&self, col: usize, row: usize) -> Data {
        self.df.columns[col].get_row_data(self.rows[row])
    }

    pub fn set_by_index(&mut self, col: usize, row: usize, data: &Data) {
        Arc::make_mut(&mut self.df).columns[col].set_row_data(self.rows[row], data)
    }

    pub fn filter_by(&mut self, col: usize, mut f: impl FnMut(usize, &Data) -> bool) {
        let col = self.col(col);
        let mut indices = vec![];

        for row_idx in 0..self.rows.len() {
            let data = col.get_row_data(row_idx);
            if f(row_idx, &data) {
                indices.push(self.rows[row_idx]);
            }
        }
        self.rows = indices;
    }

    pub fn sort_by_asc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        let cols = &self.df.columns[col];
        rows_sorted.sort_by(|a_idx, b_idx| cols.compare(*a_idx, *b_idx));
        self.rows = rows_sorted;
    }

    pub fn sort_by_desc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        let cols = &self.df.columns[col];
        rows_sorted.sort_by(|a_idx, b_idx| cols.compare(*a_idx, *b_idx).reverse());
        self.rows = rows_sorted;
    }
}
