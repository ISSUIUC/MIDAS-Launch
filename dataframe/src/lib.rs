mod view;
mod buffer;
mod data;

pub use view::{DataFrameView};
pub use buffer::DataFrame;
pub use data::{Data, DataType};


#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Shape {
    pub rows: usize,
    pub cols: usize
}

