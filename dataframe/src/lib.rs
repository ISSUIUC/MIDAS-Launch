mod view;
mod buffer;
mod data;

pub use view::{DataFrameView};
pub use buffer::{DataFrame, DataFrameBuilder};
pub use data::{Data, DataType, DataTypeNew};


#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Shape {
    pub rows: usize,
    pub cols: usize
}

