use std::cmp::Ordering;
use std::num::NonZeroU32;

use crate::{data, data::{Data, DataType}};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Shape {
    pub rows: usize,
    pub cols: usize
}

#[derive(Clone)]
pub struct ColumnInfo {
    pub offset: usize,
    pub name: String,
    pub ty: DataType,
}

#[derive(Clone)]
pub(crate) struct Header {
    // null_row: Box<[u8]>,
    // row_size: usize,
    columns: Vec<ColumnInfo>,
}

impl Header {
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    pub fn col_info(&self, col: usize) -> &ColumnInfo {
        &self.columns[col]
    }

    fn size(&self) -> usize {
        self.columns.len()
    }
}

pub struct DataFrameBuilder {
    offset: usize,
    columns: Vec<ColumnInfo>,
    context: data::Context,
}

impl DataFrameBuilder {
    pub fn new() -> Self {
        DataFrameBuilder {
            offset: 0,
            columns: vec![],
            context: data::Context::new()
        }
    }

    pub fn add_column(&mut self, name: impl Into<String>, ty: DataType) -> usize {
        let offset = self.offset;
        self.columns.push(ColumnInfo {
            offset,
            name: name.into(),
            ty
        });
        self.offset += 1;
        offset
    }

    pub fn add_interned_string(&mut self, s: impl AsRef<str>) -> NonZeroU32 {
        self.context.get_or_intern(s)
    }

    pub fn build(self) -> DataFrame {
        let layout = Header {
            columns: self.columns
        };
        DataFrame {
            mem: Vec::new(),
            rows: 0,
            context: data::Context::new(),
            header: layout
        }
    }

    pub fn build_with_capacity(self, capacity: usize) -> DataFrame {
        let layout = Header {
            columns: self.columns
        };
        DataFrame {
            mem: vec![0; capacity * layout.size()],
            rows: 0,
            context: data::Context::new(),
            header: layout
        }
    }
}

pub struct Row<'df> {
    mem: &'df [u32],
    header: &'df Header,
    ctx: &'df data::Context
}

impl<'df> Row<'df> {
    pub fn get_col_raw(&self, idx: usize) -> Option<NonZeroU32> {
        NonZeroU32::new(self.mem[idx])
    }

    pub fn get_col(&self, idx: usize) -> Data<'df> {
        let value = self.mem[idx];
        let ty = self.header.col_info(idx).ty;
        ty.to_data(value, self.ctx)
    }

    pub fn raw_slice(&self) -> &[Option<NonZeroU32>] {
        unsafe { std::mem::transmute::<&[u32], &[Option<NonZeroU32>]>(self.mem) }
    }

    pub fn iter(&self) -> impl Iterator<Item=Data<'df>> {
        let mem = self.mem;
        let header = self.header;
        let ctx = self.ctx;
        (0..header.num_cols()).map(move |idx| {
            let col_info = &header.columns[idx];
            let value = mem[idx];
            col_info.ty.to_data(value, ctx)
        })
    }
}

pub struct RowMut<'df> {
    mem: &'df mut [u32],
    header: &'df Header,
    ctx: &'df mut data::Context
}

impl<'df> RowMut<'df> {
    pub fn get_col_raw(&self, idx: usize) -> Option<NonZeroU32> {
        NonZeroU32::new(self.mem[idx])
    }

    pub fn set_col_raw(&mut self, idx: usize, value: Option<NonZeroU32>) {
        self.mem[idx] = value.map_or(0, NonZeroU32::get);
    }

    pub fn get_col(&self, idx: usize) -> Data {
        let value = self.mem[idx];
        self.header.col_info(idx).ty.to_data(value, self.ctx)
    }

    pub fn set_col(&mut self, idx: usize, value: Data<'df>) {
        self.mem[idx] = self.header.col_info(idx).ty.as_data(value, self.ctx);
    }
}


pub struct Column<'df> {
    mem: *const u32,
    len: usize,
    stride: usize,
    name: &'df str,
    ty: DataType,
    ctx: &'df data::Context
}

impl<'df> Column<'df> {
    pub fn name(&self) -> &'df str {
        self.name
    }

    pub fn data_type(&self) -> DataType {
        self.ty
    }

    pub fn get_row_raw(&self, idx: usize) -> u32 {
        debug_assert!(idx < self.len);
        unsafe { self.mem.add(idx * self.stride).read() }
    }

    pub fn get_row(&self, idx: usize) -> Data<'df> {
        self.ty.to_data(self.get_row_raw(idx), self.ctx)
    }

    pub fn compare(&self, a: usize, b: usize) -> Ordering {
        self.ty.compare(self.get_row_raw(a), self.get_row_raw(b), self.ctx)
    }
}


#[derive(Clone)]
pub struct DataFrame {
    mem: Vec<u32>,
    rows: usize,
    context: data::Context,
    header: Header
}

impl DataFrame {
    pub fn shape(&self) -> Shape {
        Shape { rows: self.rows, cols: self.header.num_cols() }
    }

    pub fn hint_rows(&mut self, rows: usize) {
        if rows * self.header.size() > self.mem.len() {
            self.mem.resize(rows * self.header.size(), 0);
        }
    }

    pub fn hint_complete(&mut self) {
        self.mem.truncate(self.rows * self.header.size());
        self.mem.shrink_to_fit();
    }

    pub fn col_names(&self) -> impl Iterator<Item=&str> {
        self.header.columns.iter().map(|col| col.name.as_str())
    }

    pub fn row(&self, index: usize) -> Row<'_> {
        debug_assert!(index < self.rows);
        let start = self.header.num_cols() * index;
        Row {
            mem: &self.mem[start..start+self.header.size()],
            header: &self.header,
            ctx: &self.context
        }
    }

    pub fn row_mut(&mut self, index: usize) -> RowMut<'_> {
        debug_assert!(index < self.rows);
        let start = self.header.num_cols() * index;
        RowMut {
            mem: &mut self.mem[start..start+self.header.size()],
            header: &self.header,
            ctx: &mut self.context
        }
    }

    pub fn col(&self, index: usize) -> Column<'_> {
        Column {
            mem: &self.mem[index] as *const u32,
            len: self.rows,
            stride: self.header.size(),
            ty: self.header.columns[index].ty,
            ctx: &self.context,
            name: &self.header.columns[index].name
        }
    }

    // pub fn col_mut(&mut self, index: usize) -> ColumnStepsMut<'_> {
    //     ColumnStepsMut {
    //         mem: &mut self.mem[index] as *mut u32,
    //         columns: self.layout.num_cols(),
    //         rows: self.rows,
    //         ty: self.layout.columns[index].ty,
    //         ctx: &mut self.layout.context,
    //         name: &self.layout.columns[index].name
    //     }
    // }

    pub fn add_null_row(&mut self) -> usize {
        if self.rows * self.header.size() < self.mem.len() {
            let idx = self.rows;
            self.rows += 1;
            idx
        } else {
            let idx = self.rows;
            self.mem.extend((0..self.header.columns.len()).map(|_| 0u32));
            self.rows += 1;
            idx
        }
    }

    pub fn add_row(&mut self, datas: &[Data]) -> usize {
        assert_eq!(datas.len(), self.header.num_cols());
        let idx = self.add_null_row();
        let mut row = self.row_mut(idx);
        for (col_idx, data) in datas.into_iter().enumerate() {
            row.set_col(col_idx, *data);
        }
        idx
    }
}

