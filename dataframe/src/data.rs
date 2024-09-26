use std::fmt::{Display, Formatter};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::num::{NonZeroU32};
use std::ops::{RangeBounds, Bound};

use ahash::AHashMap;

struct Interner {
    map: AHashMap<&'static str, NonZeroU32>,
    interned: Vec<Box<str>>
}

pub(crate) struct Context {
    interner: Interner
}

impl Context {
    pub(crate) fn new() -> Context {
        Context {
            interner: Interner {
                map: AHashMap::new(),
                interned: vec![String::from("").into_boxed_str()]
            }
        }
    }

    pub(crate) fn get_or_intern(&mut self, s: impl AsRef<str>) -> NonZeroU32 {
        let s = s.as_ref();
        if let Some(&key) = self.interner.map.get(s) {
            key
        } else {
            let key = unsafe { NonZeroU32::new_unchecked(self.interner.interned.len() as u32) };
            let storage = String::from(s).into_boxed_str();
            self.interner.map.insert(unsafe { std::mem::transmute(storage.as_ref()) }, key);
            self.interner.interned.push(storage);
            key
        }
    }

    pub(crate) fn resolve(&self, sym: NonZeroU32) -> Option<&str> {
        self.interner.interned.get(sym.get() as usize).map(|storage| {
            unsafe { std::mem::transmute(storage.as_ref()) }
        })
    }
}


impl Clone for Context {
    fn clone(&self) -> Self {
        let mut ctx = Context::new();
        for s in &self.interner.interned[1..] {
            ctx.get_or_intern(s);
        }
        ctx
    }
}


#[derive(Copy, Clone, Default, Debug)]
pub enum Data<'a> {
    #[default]
    Null,
    Integer(i32),
    Str(&'a str),
    Float(f32),
}

impl<'a> Data<'a> {
    pub fn as_integer(&self) -> Option<i32> {
        match *self {
            Data::Integer(num) => Some(num),
            Data::Str(s) => s.parse::<i32>().ok(),
            Data::Float(num) => Some(num as i32),
            Data::Null => None
        }
    }

    pub fn as_float(&self) -> Option<f32> {
        match *self {
            Data::Integer(num) => Some(num as f32),
            Data::Str(s) => s.parse::<f32>().ok(),
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
    Intern
}

impl DataType {
    pub fn parse_str<'a>(&self, s: &'a str) -> Data<'a> {
        match self {
            DataType::Integer => s.parse::<i32>().ok().map_or(Data::Null, |num| Data::Integer(num)),
            DataType::Float => s.parse::<f32>().ok().map_or(Data::Null, |num| Data::Float(num)),
            DataType::Intern => Data::Str(s)
        }
    }

    fn convert_integer(bits: NonZeroU32) -> i32 {
        (!bits.get() as i32).wrapping_add(2)
    }

    fn convert_float(bits: NonZeroU32) -> f32 {
        f32::from_bits(!bits.get())
    }

    fn convert_intern(bits: NonZeroU32, ctx: &Context) -> &str {
        ctx.resolve(bits).unwrap_or("<unknown>")
    }

    pub fn unconvert_integer(num: i32) -> u32 {
        (!num as u32).wrapping_add(2)
    }

    fn unconvert_float(num: f32) -> u32 {
        !num.to_bits()
    }

    fn unconvert_intern(s: &str, ctx: &mut Context) -> u32 {
        ctx.get_or_intern(s).get()
    }

    pub(crate) fn to_data<'df>(&self, bits: u32, ctx: &'df Context) -> Data<'df> {
        if let Some(bits) = NonZeroU32::new(bits) {
            match self {
                DataType::Integer => {
                    Data::Integer(Self::convert_integer(bits))
                },
                DataType::Float => {
                    Data::Float(Self::convert_float(bits))
                },
                DataType::Intern => {
                    Data::Str(Self::convert_intern(bits, ctx))
                }
            }
        } else {
            Data::Null
        }
    }

    pub(crate) fn as_data(&self, data: Data, ctx: &mut Context) -> u32 {
        match self {
            DataType::Integer => {
                if let Data::Integer(num) = data {
                    Self::unconvert_integer(num)
                } else {
                    0u32
                }
            }
            DataType::Float => {
                if let Data::Float(num) = data {
                    Self::unconvert_float(num)
                } else if let Data::Integer(num) = data {
                    Self::unconvert_float(num as f32)
                } else {
                    0u32
                }
            }
            DataType::Intern => {
                if let Data::Str(s) = data {
                    Self::unconvert_intern(s, ctx)
                } else {
                    0u32
                }
            }
        }
    }

    pub(crate) fn compare(&self, a: u32, b: u32, ctx: &Context) -> Ordering {
        match (NonZeroU32::new(a), NonZeroU32::new(b)) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(a), Some(b)) => {
                match self {
                    DataType::Integer => Self::convert_integer(a).cmp(&Self::convert_integer(b)),
                    DataType::Float => Self::convert_float(a).total_cmp(&Self::convert_float(b)),
                    DataType::Intern => Self::convert_intern(a, ctx).cmp(&Self::convert_intern(b, ctx)),
                }
            }
        }
    }
}