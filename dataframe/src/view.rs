use std::{io, io::BufRead};
use std::sync::Arc;

use crate::data::{Data, DataType};
use crate::frame::{DataFrame, DataFrameBuilder, Row, RowMut, Shape, Column};


pub struct ColumnView<'v> {
    rows: &'v Vec<usize>,
    col: Column<'v>
}

impl<'v> ColumnView<'v> {
    pub fn name(&self) -> &'v str {
        self.col.name()
    }

    pub fn get_row(&self, idx: usize) -> Data<'v> {
        self.col.get_row(self.rows[idx])
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
            df: Arc::new(df),
        }
    }

    pub fn from_csv(file: &mut impl BufRead, mut on_row_callback: impl FnMut(usize)) -> io::Result<Self> {
        let mut offset = 0;
        let mut header = String::new();
        let mut row_numbers = Vec::new();
        offset += file.read_line(&mut header)?;
        if header.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }

        let mut dataframe_builder = DataFrameBuilder::new();
        let mut data_types = Vec::new();

        let mut row_buf = String::new();
        offset += file.read_line(&mut row_buf)?;
        if row_buf.is_empty() {
            for col_name in header.trim().split(',') {
                dataframe_builder.add_column(col_name.trim(), DataType::Intern);
            }
            let df = dataframe_builder.build();
            return Ok(DataFrameView {
                rows: row_numbers,
                df: Arc::new(df)
            });
        }

        for (col_name, item) in header.trim().split(',').zip(row_buf.trim().split(',')) {
            let item = item.trim();
            let col_name = col_name.trim();

            if let Ok(_) = item.parse::<f32>() {
                dataframe_builder.add_column(col_name, DataType::Float);
                data_types.push(DataType::Float);
            } else {
                dataframe_builder.add_column(col_name, DataType::Intern);
                data_types.push(DataType::Intern);
            }
        }
        let mut df = dataframe_builder.build();
        let mut row_data = vec![];
        for (ty, item) in data_types.iter().zip(row_buf.trim().split(',')) {
            row_data.push(ty.parse_str(item.trim()));
        }
        if row_data.len() != df.shape().cols {
            return Err(io::Error::other("Malformed CSV file."));
        }
        row_numbers.push(df.add_row(&row_data));
        on_row_callback(offset);

        loop {
            let mut row_data = Vec::new();
            row_buf.clear();
            let amount = file.read_line(&mut row_buf)?;
            if row_buf.is_empty() {
                return Ok(DataFrameView {
                    rows: row_numbers,
                    df: Arc::new(df)
                })
            }
            offset += amount;
            for (dtype, item) in data_types.iter().zip(row_buf.trim_end_matches('\n').split(',')) {
                row_data.push(dtype.parse_str(item.trim()));
            }
            if row_data.len() != df.shape().cols {
                return Err(io::Error::other("Malformed CSV file."));
            }
            row_numbers.push(df.add_row(&row_data));
            on_row_callback(offset);
        }
    }

    pub fn shape(&self) -> Shape {
        Shape {
            rows: self.rows.len(),
            cols: self.df.shape().cols
        }
    }

    pub fn col_names(&self) -> impl Iterator<Item=&str> {
        self.df.col_names()
    }

    pub fn col_name(&self, idx: usize) -> &str {
        self.df.col(idx).name()
    }

    pub fn col(&self, idx: usize) -> ColumnView {
        ColumnView {
            rows: &self.rows,
            col: self.df.col(idx)
        }
    }

    pub fn row(&self, idx: usize) -> Row {
        self.df.row(self.rows[idx])
    }

    pub fn row_mut(&mut self, idx: usize) -> RowMut {
        Arc::make_mut(&mut self.df).row_mut(self.rows[idx])
    }

    // pub fn col_mut(&mut self, idx: usize) -> ColumnViewMut<'_, impl ColumnMut> {
    //     ColumnViewMut(&mut self.rows, self.df.col_mut(idx))
    // }

    // pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
    //     self.df.row(index).iter()
    //     // self.df.row_iter(self.rows[index])
    // }

    pub fn get_by_index(&self, col: usize, row: usize) -> Data {
        self.df.row(self.rows[row]).get_col(col)
    }

    pub fn set_by_index(&mut self, col: usize, row: usize, data: Data) {
        Arc::make_mut(&mut self.df).row_mut(self.rows[row]).set_col(col, data)
    }

    pub fn filter_by(&mut self, col: usize, mut f: impl FnMut(usize, &Data) -> bool) {
        let indices = {
            let col = self.col(col);
            let mut indices = vec![];

            for row_idx in 0..self.rows.len() {
                let data = col.get_row(row_idx);
                if f(row_idx, &data) {
                    indices.push(self.rows[row_idx]);
                }
            }
            indices
        };
        self.rows = indices;
    }

    pub fn sort_by_asc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        let col = &self.df.col(col);
        rows_sorted.sort_by(|&a_idx, &b_idx| col.compare(a_idx, b_idx));
        self.rows = rows_sorted;
    }

    pub fn sort_by_desc(&mut self, col: usize) {
        let mut rows_sorted = self.rows.clone();
        let col = &self.df.col(col);
        rows_sorted.sort_by(|a_idx, b_idx| col.compare(*a_idx, *b_idx).reverse());
        self.rows = rows_sorted;
    }
}
