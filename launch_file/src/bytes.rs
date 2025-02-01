use byteorder::{LittleEndian, ReadBytesExt};
use indexmap::IndexMap;
use crate::deserialize::SerializedCpp;


struct FormatHeaderParser<'a>(&'a [u8]);

impl<'a> FormatHeaderParser<'a> {
    fn read_u8(&mut self) -> Option<u8> {
        self.0.read_u8().ok()
    }

    fn read_u32(&mut self) -> Option<u32> {
        self.0.read_u32::<LittleEndian>().ok()
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if let Some((value, remaining)) = self.0.split_at_checked(n) {
            self.0 = remaining;
            Some(value)
        } else {
            None
        }
    }

    fn read_pascal_string(&mut self) -> Option<&'a str> {
        let length = self.read_u8()? as usize;
        let slice = self.take(length)?;
        std::str::from_utf8(slice).ok()
    }

    fn read_type(&mut self) -> Option<SerializedCpp> {
        let byte = self.read_u8()?;
        match byte >> 5 {
            0b000 => {
                let signed = (byte & 0b00010000) == 1;
                let size = byte & 0b1111;
                Some(SerializedCpp::Integer { signed, size })
            }
            0b001 => {
                Some(SerializedCpp::Boolean)
            }
            0b010 => {
                let size = byte & 0b11111;
                Some(SerializedCpp::Float { size })
            }
            0b011 => {
                let member_count = byte & 0b11111;
                let mut members = IndexMap::new();
                for _ in 0..member_count {
                    let member_name = self.read_pascal_string()?.to_owned();
                    let member_type = self.read_type()?;
                    members.insert(member_name, member_type);
                }
                Some(SerializedCpp::Struct { members })
            }
            0b100 => {
                let count = (byte & 0b11111) as u32;
                let item = Box::new(self.read_type()?);
                Some(SerializedCpp::Array { count, item })
            }
            0b101 => {
                let variant_count = byte & 0b11111;
                let mut variants = IndexMap::new();
                for _ in 0..variant_count {
                    let discriminant = self.read_u32()?;
                    let name = self.read_pascal_string()?.to_string();
                    variants.insert(name, discriminant);
                }
                Some(SerializedCpp::Enum { variants })
            }
            0b110 => unimplemented!(),
            _ => unreachable!()
        }
    }

    fn parse(&mut self) -> Option<IndexMap<String, (u32, SerializedCpp)>> {
        let num_variants = self.read_u8()?;
        let mut variants = IndexMap::new();
        for _ in 0..num_variants {
            let discriminant = self.read_u8()? as u32;
            let name = self.read_pascal_string()?.to_owned();
            let ty = self.read_type()?;
            variants.insert(name, (discriminant, ty));
        }
        Some(variants)
    }
}


pub fn from_inline_header_helper(data: &[u8]) -> Option<IndexMap<String, (u32, SerializedCpp)>> {
    FormatHeaderParser(data).parse()
}