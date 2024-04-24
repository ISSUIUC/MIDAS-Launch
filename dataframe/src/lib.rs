mod column;
mod view;
mod buffer;
mod data;

pub use view::{DataFrameView, ColumnView, ColumnViewMut};
pub use buffer::DataFrame;
pub use data::{Data, DataType};
pub use column::{Column, ColumnMut};


#[derive(Copy, Clone, Debug)]
pub struct Shape {
    pub rows: usize,
    pub cols: usize
}

