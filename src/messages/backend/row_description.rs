use std::{error::Error, io::Read};

use crate::{messages::Message, readers::*};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RowDescription {
    fields: Vec<Field>,
}

impl RowDescription {
    pub fn builder() -> RowDescriptionBuilder {
        RowDescriptionBuilder { fields: Vec::new() }
    }
}

pub struct RowDescriptionBuilder {
    fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    name: String,
    table_oid: u32,
    column_index: u16,
    data_type_oid: u32,
    data_type_size: u16,
    type_modifier: u32,
    format_code: u16,
}

impl RowDescriptionBuilder {
    pub fn string_field(mut self, name: impl Into<String>) -> Self {
        let field = Field {
            name: name.into(),
            table_oid: 0,
            column_index: 0,
            data_type_oid: 0,
            data_type_size: 0,
            type_modifier: 0,
            format_code: 0,
        };

        self.fields.push(field);
        self
    }

    pub fn build(self) -> RowDescription {
        RowDescription {
            fields: self.fields,
        }
    }
}

impl RowDescription {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let field_count = read_u16(stream)? as usize;
        let mut fields: Vec<Field> = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = Field {
                name: read_string(stream)?,
                table_oid: read_u32(stream)?,
                column_index: read_u16(stream)?,
                data_type_oid: read_u32(stream)?,
                data_type_size: read_u16(stream)?,
                type_modifier: read_u32(stream)?,
                format_code: read_u16(stream)?,
            };

            fields.push(field);
        }

        Ok(Self { fields })
    }

    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.to_string()).collect()
    }
}

impl Message for RowDescription {
    fn encode(&self) -> Vec<u8> {
        let mut field_buffer = Vec::new();
        for field in &self.fields {
            // Field Name
            field_buffer.extend_from_slice(&field.name.as_bytes());
            field_buffer.push(0);

            // Table OID (u32) or zero
            field_buffer.extend_from_slice(&field.table_oid.to_be_bytes());

            // Column Index (u16) or zero
            field_buffer.extend_from_slice(&field.column_index.to_be_bytes());

            // Data Type OID (u32)
            field_buffer.extend_from_slice(&field.data_type_oid.to_be_bytes());

            // Data Type Size (i16). Negative values denote variable length types.
            field_buffer.extend_from_slice(&field.data_type_size.to_be_bytes());

            // Type Modifier (u32). Type-dependent field.
            field_buffer.extend_from_slice(&field.type_modifier.to_be_bytes());

            // Format Code (u16). 0 = text (or unknown), 1 = binary
            field_buffer.extend_from_slice(&field.format_code.to_be_bytes());
        }

        let mut buffer = Vec::new();
        buffer.push(b'T');

        // Length of message contents in bytes, including self.
        buffer.extend_from_slice(&(field_buffer.len() as u32 + 4 + 2).to_be_bytes());
        // Number of fields in the row.
        buffer.extend_from_slice(&(self.fields.len() as u16).to_be_bytes());
        // The fields serialized
        buffer.extend_from_slice(&field_buffer);

        buffer
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::backend::BackendMessage;
    use std::io::Cursor;

    #[test]
    fn test_empty_row_description() -> Result<(), Box<dyn Error>> {
        let row_description = RowDescription::builder().build();

        let encoded = row_description.encode();
        assert_eq!(encoded.len(), 7);
        assert_eq!(
            encoded,
            vec![
                // message tag
                b'T', // length
                0x00, 0x00, 0x00, 6, // field count
                0x00, 0x00,
            ]
        );

        let mut cursor = Cursor::new(encoded);
        let decoded = BackendMessage::read_next_message(&mut cursor)?;
        assert_eq!(decoded, BackendMessage::RowDescription(row_description));

        Ok(())
    }

    #[test]
    fn test_single_row_description() -> Result<(), Box<dyn Error>> {
        let row_description = RowDescription::builder().string_field("id").build();

        let encoded = row_description.encode();
        assert_eq!(encoded.len(), 28);
        assert_eq!(
            encoded,
            vec![
                // tag
                b'T', // length
                0x00, 0x00, 0x00, 27, // field count
                0x00, 0x01, // field name, null terminated
                b'i', b'd', 0x00, // table oid
                0x00, 0x00, 0x00, 0x00, // column index
                0x00, 0x00, // data type oid
                0x00, 0x00, 0x00, 0x00, // data type size
                0x00, 0x00, // type modifier
                0x00, 0x00, 0x00, 0x00, // format code
                0x00, 0x00,
            ]
        );

        let mut cursor = Cursor::new(encoded);
        let decoded = BackendMessage::read_next_message(&mut cursor)?;
        assert_eq!(decoded, BackendMessage::RowDescription(row_description));

        Ok(())
    }

    #[test]
    fn test_multi_row_description() -> Result<(), Box<dyn Error>> {
        let row_description = RowDescription::builder()
            .string_field("id")
            .string_field("name")
            .build();

        let encoded = row_description.encode();
        assert_eq!(encoded.len(), 51);
        assert_eq!(
            encoded,
            vec![
                // tag
                b'T', // length
                0x00, 0x00, 0x00, 50, // field count
                0x00, 0x02, // `id`
                // field name, null terminated
                b'i', b'd', 0x00, // table oid
                0x00, 0x00, 0x00, 0x00, // column index
                0x00, 0x00, // data type oid
                0x00, 0x00, 0x00, 0x00, // data type size
                0x00, 0x00, // type modifier
                0x00, 0x00, 0x00, 0x00, // format code
                0x00, 0x00, // `name`
                // field name, null terminated
                b'n', b'a', b'm', b'e', 0x00, // table oid
                0x00, 0x00, 0x00, 0x00, // column index
                0x00, 0x00, // data type oid
                0x00, 0x00, 0x00, 0x00, // data type size
                0x00, 0x00, // type modifier
                0x00, 0x00, 0x00, 0x00, // format code
                0x00, 0x00,
            ]
        );

        let mut cursor = Cursor::new(encoded);
        let decoded = BackendMessage::read_next_message(&mut cursor)?;
        assert_eq!(decoded, BackendMessage::RowDescription(row_description));

        Ok(())
    }
}
