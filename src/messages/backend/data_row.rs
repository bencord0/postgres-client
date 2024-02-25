use crate::readers::*;
use std::{error::Error, io::Read, str};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRow {
    pub fields: Vec<Option<String>>,
}

impl DataRow {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let field_count = read_u16(stream)? as usize;
        let mut fields: Vec<Option<String>> = vec![None; field_count as usize];

        for index in 0..field_count {
            let field_length = read_u32(stream)? as usize;

            match field_length {
                0xFFFFFFFF => {
                    continue;
                }
                size => {
                    let field_value = read_bytes(size, stream)?;
                    fields[index] = Some(str::from_utf8(&field_value)?.to_string());
                }
            }
        }

        Ok(DataRow { fields })
    }
}

impl DataRow {
    pub fn builder() -> DataRowBuilder {
        DataRowBuilder { fields: Vec::new() }
    }
}

pub struct DataRowBuilder {
    fields: Vec<Option<String>>,
}

impl DataRowBuilder {
    pub fn string_field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(Some(field.into()));
        self
    }

    pub fn null_field(mut self) -> Self {
        self.fields.push(None);
        self
    }

    pub fn build(self) -> DataRow {
        DataRow {
            fields: self.fields,
        }
    }
}
