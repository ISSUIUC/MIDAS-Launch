use std::cmp::Ordering;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use smallvec::{SmallVec, smallvec};

use super::Shape;
use super::column::{Column, ColumnInternal, ColumnMut, ColumnMutInternal};
use super::data::{ColumnData, Data, DataType, Enum, Float, Integer};


// pub type DataFrame = DataFrameOld;
pub type DataFrame = DataFrameNew;

#[derive(Clone)]
struct DataItem {
    which: u8,
    mem: [u8;8]
}

impl DataItem {
    fn as_data(&self, ty: DataType) -> Data {
        unsafe { match ty {
            DataType::Integer => Data::Integer(transmute(self.mem)),
            DataType::Float => Data::Float(transmute(self.mem)),
            DataType::Enum => Data::Str("Nostr")
        } }
    }

    fn new(x: Data, which: u8) -> Self {
        let buff: [u8;8] = unsafe { match x {
            Data::Integer(i) => transmute(i),
            Data::Str(_) => transmute(0i64),
            Data::Float(f) => transmute(f),
            Data::Null => [0;8]
        }};

        Self {
            which,
            mem: buff
        }
    }
}

#[derive(Clone)]
pub struct DataFrameNew {
    pub(crate) columns: Vec<NewColumn>,
    items: Vec<SmallVec<[DataItem;5]>>
}

impl DataFrameNew {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            items: vec![]
        }
    }

    pub fn shape(&self) -> Shape {
        Shape { rows: self.items.len(), cols: self.columns.len() }
    }

    pub fn hint_complete(&mut self) {}

    pub fn add_null_col(&mut self, name: impl Into<String>, ty: DataType) -> usize {
        self.columns.push(NewColumn{name: name.into(),which:0,ty});
        assert!(self.columns.len() < u8::MAX as usize);
        self.columns.len() - 1

    }

    pub fn add_null_row(&mut self, row: &[Data]) {
        self.items.push(smallvec![]);
    }

    pub fn add_row(&mut self, row: &[Data]) {
        self.items.push(row.iter().enumerate()
                            .filter(|(i,x)|!x.is_null())
                            .map(|(i,&x)|DataItem::new(x,i as u8))
                            .collect()
        );
    }

    pub fn cols(&self) -> &[impl Column] {
        &self.columns
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        let row = &self.items[index];
        self.columns.iter().enumerate().map(|(i,c)|{
            if let Some(item) = row.iter().find(|x|x.which as usize == i) {
                item.as_data(c.ty)
            } else {
                Data::Null
            }
        })
    }
}

#[derive(Clone)]
pub struct NewColumn {
    name: String,
    which: u32,
    ty: DataType,
}

impl Column for NewColumn {
    fn name(&self) -> &str {
        &self.name
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn data_type(&self) -> DataType {
        todo!()
    }

    fn get_row_data(&self, index: usize) -> Data {
        todo!()
    }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        todo!()
    }
}

impl ColumnMut for NewColumn {
    fn set_row_data(&mut self, index: usize, data: &Data) {
        todo!()
    }
}

#[derive(Clone)]
pub struct DataFrameOld {
    pub(crate) columns: Vec<ColumnVariants>,
    pub(crate) rows: usize
}

impl DataFrameOld {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            rows: 0
        }
    }

    pub fn shape(&self) -> Shape {
        Shape { rows: self.rows, cols: self.columns.len() }
    }

    pub fn hint_complete(&mut self) {
        for col in &mut self.columns {
            col.hint_complete();
        }
    }

    pub fn add_null_col(&mut self, name: impl Into<String>, ty: DataType) -> usize {
        let idx = self.columns.len();
        match ty {
            DataType::Integer => self.columns.push(GenericColumn::<Integer>::new_null(name, self.rows).into()),
            DataType::Enum => self.columns.push(GenericColumn::<Enum>::new_null(name, self.rows).into()),
            DataType::Float => self.columns.push(GenericColumn::<Float>::new_null(name, self.rows).into()),
        }
        idx
    }

    pub fn add_null_row(&mut self) {
        for col in self.columns.iter_mut() {
            col.push_data(&Data::Null)
        }
        self.rows += 1;
    }

    pub fn add_row(&mut self, row: &[Data]) {
        assert_eq!(row.len(), self.columns.len());
        for (col, item) in self.columns.iter_mut().zip(row) {
            col.push_data(item);
        }
        self.rows += 1;
    }

    pub fn cols(&self) -> &[impl Column] {
        &self.columns
    }

    pub fn row_iter(&self, index: usize) -> impl Iterator<Item=Data> {
        self.columns.iter().map(move |col| col.get_row_data(index))
    }
}

#[derive(Clone)]
pub(crate) struct GenericColumn<D: ColumnData> {
    ctx: D::Context,
    name: String,
    items: Vec<D>
}

impl<D: ColumnData> GenericColumn<D> {
    fn new_null(name: impl Into<String>, rows: usize) -> GenericColumn<D> {
        GenericColumn {
            ctx: D::Context::default(),
            name: name.into(),
            items: vec![D::null(); rows]
        }
    }

    fn push(&mut self, item: D) {
        self.items.push(item);
    }
}

impl<D: ColumnData> Column for GenericColumn<D> {
    fn name(&self) -> &str {
        &self.name
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn data_type(&self) -> DataType {
        D::TYPE
    }

    fn get_row_data(&self, index: usize) -> Data {
        self.items[index].to_data(&self.ctx)
    }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.items[a].compare(&self.items[b], &self.ctx)
    }
}

impl<D: ColumnData> ColumnMut for GenericColumn<D> {
    fn set_row_data(&mut self, index: usize, data: &Data) {
        let d= D::from_data(data, &mut self.ctx).unwrap_or_else(|| D::null());
        self.items[index] = d;
    }
}

impl<D: ColumnData> ColumnInternal for GenericColumn<D> {
    fn underlying_rows(&self) -> usize { self.items.len() }
}

impl<D: ColumnData> ColumnMutInternal for GenericColumn<D> {
    fn hint_complete(&mut self) {
        self.items.shrink_to_fit();
    }

    fn push_data(&mut self, item: &Data) {
        let d= D::from_data(item, &mut self.ctx).unwrap_or_else(|| D::null());
        self.push(d);
    }
}

#[derive(Clone)]
pub(crate) enum ColumnVariants {
    Integer(GenericColumn<Integer>),
    Enum(GenericColumn<Enum>),
    Float(GenericColumn<Float>),
}

impl Column for ColumnVariants {
    fn name(&self) -> &str { self.deref().name() }
    fn len(&self) -> usize { self.deref().len() }
    fn data_type(&self) -> DataType { self.deref().data_type() }
    fn get_row_data(&self, index: usize) -> Data { self.deref().get_row_data(index) }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        self.deref().compare(a, b)
    }
}

impl ColumnMut for ColumnVariants {
    fn set_row_data(&mut self, index: usize, data: &Data) { self.deref_mut().set_row_data(index, data) }
}

impl ColumnInternal for ColumnVariants {
    fn underlying_rows(&self) -> usize { self.deref().underlying_rows() }
}

impl ColumnMutInternal for ColumnVariants {
    fn hint_complete(&mut self) { self.deref_mut().hint_complete() }
    fn push_data(&mut self, item: &Data) { self.deref_mut().push_data(item) }
}

impl Deref for ColumnVariants {
    type Target = dyn ColumnMutInternal;

    fn deref(&self) -> &(dyn ColumnMutInternal + 'static) {
        match self {
            ColumnVariants::Integer(col) => col,
            ColumnVariants::Enum(col) => col,
            ColumnVariants::Float(col) => col,
        }
    }
}

impl DerefMut for ColumnVariants {
    fn deref_mut(&mut self) -> &mut (dyn ColumnMutInternal + 'static) {
        match self {
            ColumnVariants::Integer(col) => col,
            ColumnVariants::Enum(col) => col,
            ColumnVariants::Float(col) => col,
        }
    }
}

impl From<GenericColumn<Integer>> for ColumnVariants {
    fn from(value: GenericColumn<Integer>) -> Self {
        ColumnVariants::Integer(value)
    }
}

impl From<GenericColumn<Enum>> for ColumnVariants {
    fn from(value: GenericColumn<Enum>) -> Self {
        ColumnVariants::Enum(value)
    }
}

impl From<GenericColumn<Float>> for ColumnVariants {
    fn from(value: GenericColumn<Float>) -> Self {
        ColumnVariants::Float(value)
    }
}
