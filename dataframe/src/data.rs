use string_interner::{DefaultSymbol, StringInterner};
use string_interner::backend::StringBackend;

use std::fmt::{Display, Formatter};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::ops::{RangeBounds, Bound};
use byteorder::{WriteBytesExt, ReadBytesExt, LittleEndian};
use crate::data;

pub trait ColumnData: Copy + Eq + 'static {
    const TYPE: DataType;

    type Context: Default;

    fn null() -> Self;
    fn is_null(&self) -> bool {
        self.eq(&Self::null())
    }

    fn to_data<'a>(&'a self, ctx: &'a Self::Context) -> Data<'a>;
    fn from_data(data: &Data, ctx: &mut Self::Context) -> Option<Self>;

    fn compare_not_null(&self, other: &Self, ctx: &Self::Context) -> Ordering;
    fn compare(&self, other: &Self, ctx: &Self::Context) -> Ordering {
        if self.is_null() && other.is_null() {
            Ordering::Equal
        } else if self.is_null() {
            Ordering::Less
        } else if other.is_null() {
            Ordering::Greater
        } else {
            self.compare_not_null(other, ctx)
        }
    }
}

#[derive(Copy, Clone, Default, Debug)]
pub enum Data<'a> {
    Integer(i64),
    Str(&'a str),
    Float(f64),
    #[default]
    Null
}

impl<'a> Data<'a> {
    pub fn as_integer(&self) -> Option<i64> {
        match *self {
            Data::Integer(num) => Some(num),
            Data::Str(s) => s.parse::<i64>().ok(),
            Data::Float(num) => Some(num as i64),
            Data::Null => None
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match *self {
            Data::Integer(num) => Some(num as f64),
            Data::Str(s) => s.parse::<f64>().ok(),
            Data::Float(num) => Some(num),
            Data::Null => None
        }
    }

    pub fn as_str(&self) -> Option<Cow<str>> {
        match *self {
            Data::Integer(num) => Some(num.to_string().into()),
            Data::Str(s) => Some(s.into()),
            Data::Float(num) => Some(num.to_string().into()),
            Data::Null => None
        }
    }

    pub fn eq(&self, other: &Data) -> bool {
        match (self, other) {
            (Data::Null, Data::Null) => true,
            (Data::Integer(a), Data::Integer(b)) => a == b,
            (Data::Float(a), Data::Float(b)) => a.total_cmp(b).is_eq(),
            (Data::Str(a), Data::Str(b)) => a == b,
            _ => false
        }
    }

    pub fn compare(&self, other: &Data) -> Option<Ordering> {
        match (self, other) {
            (Data::Null, Data::Null) => Some(Ordering::Equal),
            (Data::Integer(a), Data::Integer(b)) => Some(a.cmp(b)),
            (Data::Float(a), Data::Float(b)) => Some(a.total_cmp(b)),
            (Data::Str(a), Data::Str(b)) => Some(a.cmp(b)),
            _ => None
        }
    }

    pub fn in_bounds<'b>(&self, range: impl RangeBounds<Data<'b>>) -> bool {
        match range.start_bound() {
            Bound::Included(value) => {
                if self.compare(value).map_or(false, |ord| ord.is_lt()) {
                    return false;
                }
            }
            Bound::Excluded(value) => {
                if self.compare(value).map_or(false, |ord| ord.is_le()) {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        match range.end_bound() {
            Bound::Included(value) => {
                if self.compare(value).map_or(false, |ord| ord.is_gt()) {
                    return false;
                }
            }
            Bound::Excluded(value) => {
                if self.compare(value).map_or(false, |ord| ord.is_ge()) {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        true
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Data::Null)
    }
}

impl Display for Data<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Integer(num) => {
                write!(f, "{}", num)
            }
            Data::Str(s) => {
                write!(f, "{}", s)
            }
            Data::Float(num) => {
                write!(f, "{}", num)
            }
            Data::Null => {
                write!(f, "")
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DataType {
    Integer,
    Float,
    Enum
}


#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum DataTypeNew {
    Bool,
    I8,
    I32,
    U8,
    U32,
    F32,
    F64,
    Enum,
}

impl DataTypeNew {
    pub fn width(&self) -> usize {
        match self {
            DataTypeNew::Bool => 1,
            DataTypeNew::I8 => 1,
            DataTypeNew::I32 => 4,
            DataTypeNew::U8 => 1,
            DataTypeNew::U32 => 4,
            DataTypeNew::F32 => 4,
            DataTypeNew::F64 => 8,
            DataTypeNew::Enum => 4,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DataTypeNew::Bool => DataType::Integer,
            DataTypeNew::I8 => DataType::Integer,
            DataTypeNew::I32 => DataType::Integer,
            DataTypeNew::U8 => DataType::Integer,
            DataTypeNew::U32 => DataType::Integer,
            DataTypeNew::F32 => DataType::Float,
            DataTypeNew::F64 => DataType::Float,
            DataTypeNew::Enum => DataType::Enum
        }
    }

    pub fn read(&self, mut mem: &[u8]) -> Data {
        match self {
            DataTypeNew::Bool => Data::Integer(mem.read_u8().unwrap() as i64),
            DataTypeNew::I8 => Data::Integer(mem.read_i8().unwrap() as i64),
            DataTypeNew::I32 => Data::Integer(mem.read_i32::<LittleEndian>().unwrap() as i64),
            DataTypeNew::U8 => Data::Integer(mem.read_u8().unwrap() as i64),
            DataTypeNew::U32 => Data::Integer(mem.read_u32::<LittleEndian>().unwrap() as i64),
            DataTypeNew::F32 => Data::Float(mem.read_f32::<LittleEndian>().unwrap() as f64),
            DataTypeNew::F64 => Data::Float(mem.read_f64::<LittleEndian>().unwrap() as f64),
            DataTypeNew::Enum => Data::Str("_"),
        }
    }

    pub fn write(&self, data: &Data, mut mem: &mut [u8]) {
        match (self,data) {
            (DataTypeNew::Bool, Data::Integer(i)) => mem.write_u8(*i as u8),
            (DataTypeNew::I8, Data::Integer(i)) => mem.write_i8(*i as i8),
            (DataTypeNew::I32, Data::Integer(i)) => mem.write_i32::<LittleEndian>(*i as i32),
            (DataTypeNew::U8, Data::Integer(i)) => mem.write_u8(*i as u8),
            (DataTypeNew::U32, Data::Integer(i)) => mem.write_u32::<LittleEndian>(*i as u32),
            (DataTypeNew::F32, Data::Float(f)) => mem.write_f32::<LittleEndian>(*f as f32),
            (DataTypeNew::F64, Data::Float(f)) => mem.write_f64::<LittleEndian>(*f as f64),
            (DataTypeNew::Enum, Data::Str(s)) => mem.write_u32::<LittleEndian>(0),
            (_, Data::Null) => Ok(mem.fill(0xff)),
            _ => panic!()
        }.unwrap();
    }
}

impl DataType {
    pub fn parse_str<'a>(&self, s: &'a str) -> Data<'a> {
        match self {
            DataType::Integer => s.parse::<i64>().ok().map_or(Data::Null, |num| Data::Integer(num)),
            DataType::Float => s.parse::<f64>().ok().map_or(Data::Null, |num| Data::Float(num)),
            DataType::Enum => Data::Str(s)
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Integer(pub i64);

impl ColumnData for Integer {
    const TYPE: DataType = DataType::Integer;
    type Context = ();

    fn null() -> Self {
        Integer(i64::MIN)
    }

    fn to_data(&self, _ctx: &Self::Context) -> Data {
        if self.is_null() {
            Data::Null
        } else {
            Data::Integer(self.0)
        }
    }

    fn from_data(data: &Data, _ctx: &mut Self::Context) -> Option<Self> {
        if let &Data::Integer(num) = data {
            if num == i64::MIN {
                None
            } else {
                Some(Integer(num))
            }
        } else if let Data::Null = data {
            Some(Integer::null())
        } else {
            None
        }
    }

    fn compare_not_null(&self, other: &Self, _ctx: &Self::Context) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Copy, Clone)]
pub struct Float(pub f64);

impl ColumnData for Float {
    const TYPE: DataType = DataType::Float;
    type Context = ();

    fn null() -> Self {
        Float(f64::MIN_POSITIVE)
    }

    fn to_data(&self, _ctx: &Self::Context) -> Data {
        if self.is_null() {
            Data::Null
        } else {
            Data::Float(self.0)
        }
    }

    fn from_data(data: &Data, _ctx: &mut Self::Context) -> Option<Self> {
        if let &Data::Float(num) = data {
            if num == f64::MIN_POSITIVE {
                None
            } else {
                Some(Float(num))
            }
        } else if let &Data::Integer(num) = data {
            Some(Float(num as f64))
        } else if let Data::Null = data {
            Some(Float::null())
        } else {
            None
        }
    }

    fn compare_not_null(&self, other: &Self, _ctx: &Self::Context) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for Float { }


#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Enum(Option<DefaultSymbol>);

impl ColumnData for Enum {
    const TYPE: DataType = DataType::Enum;
    type Context = StringInterner<StringBackend>;

    fn null() -> Self {
        Enum(None)
    }

    fn is_null(&self) -> bool {
        self.0.is_none()
    }

    fn to_data<'a>(&'a self, ctx: &'a Self::Context) -> Data<'a> {
        if let Some(sym) = self.0 {
            Data::Str(ctx.resolve(sym).unwrap())
        } else {
            Data::Null
        }
    }

    fn from_data(data: &Data, ctx: &mut Self::Context) -> Option<Self> {
        if let &Data::Str(s) = data {
            Some(Enum(Some(ctx.get_or_intern(s))))
        } else if let Data::Null = data {
            Some(Enum::null())
        } else {
            None
        }
    }

    fn compare_not_null(&self, other: &Self, ctx: &Self::Context) -> Ordering {
        let a= ctx.resolve(self.0.unwrap()).unwrap();
        let b= ctx.resolve(other.0.unwrap()).unwrap();
        a.cmp(b)
    }
}