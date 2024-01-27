use std::{
    error::Error,
    io::{Cursor, Read},
    str,
};

use crate::{messages::Message, readers::*, state::TransactionStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    ReadyForQuery(ReadyForQuery),
    RowDescription(RowDescription),
    DataRow(DataRow),
    CommandComplete(CommandComplete),
    Error { length: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyForQuery {
    pub transaction_status: TransactionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescription {
    pub fields: Vec<String>,
}

impl RowDescription {
    pub fn builder() -> RowDescriptionBuilder {
        RowDescriptionBuilder { fields: Vec::new() }
    }
}

pub struct RowDescriptionBuilder {
    fields: Vec<String>,
}

impl RowDescriptionBuilder {
    pub fn string_field(mut self, field: impl Into<String>) -> Self {
        self.fields.push(field.into());
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
        let mut fields: Vec<String> = vec![String::new(); field_count];
        for index in 0..field_count {
            let field_name = read_string(stream)?;
            let _table_oid = read_u32(stream)?;
            let _column_index = read_u16(stream)?;
            let _data_type_oid = read_u32(stream)?;
            let _data_type_size = read_u16(stream)?;
            let _type_modifier = read_u32(stream)?;
            let _format_code = read_u16(stream)?;

            fields[index] = field_name;
        }

        Ok(Self { fields })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRow {
    pub fields: Vec<Option<String>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandComplete {
    pub tag: String,
}

impl CommandComplete {
    pub fn builder() -> CommandCompleteBuilder {
        CommandCompleteBuilder { tag: None }
    }

    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let tag = read_string(stream)?;
        Ok(Self { tag })
    }
}

pub struct CommandCompleteBuilder {
    tag: Option<String>,
}

impl CommandCompleteBuilder {
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn build(self) -> CommandComplete {
        CommandComplete {
            tag: self.tag.unwrap_or_default(),
        }
    }
}

impl BackendMessage {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let mut header: Vec<u8> = vec![0; 5];
        let bytes_read = stream.read(&mut header)?;
        if bytes_read != 5 {
            return Err("expected 5 bytes for message type".into());
        }

        let r#type: u8 = header[0];
        let length: u32 = u32::from_be_bytes(header[1..5].try_into()?);
        let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);

        let message: BackendMessage = match r#type {
            b'Z' => BackendMessage::ReadyForQuery(ReadyForQuery {
                transaction_status: TransactionStatus::from_u8(read_u8(&mut buffer)?),
            }),
            b'T' => BackendMessage::RowDescription(RowDescription::read_next_message(&mut buffer)?),
            b'D' => BackendMessage::DataRow(DataRow::read_next_message(&mut buffer)?),
            b'C' => {
                BackendMessage::CommandComplete(CommandComplete::read_next_message(&mut buffer)?)
            }
            b'E' => {
                let _ = read_bytes(length as usize - 4, stream)?;
                BackendMessage::Error { length }
            }
            _ => {
                return Err(
                    format!("unhandled message type: {:?}", str::from_utf8(&[r#type])?).into(),
                );
            }
        };

        Ok(message)
    }
}

impl Message for ReadyForQuery {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'Z');
        buffer.extend_from_slice(&5u32.to_be_bytes());
        buffer.extend_from_slice(&[self.transaction_status.to_u8()]);
        buffer
    }
}

impl Message for RowDescription {
    fn encode(&self) -> Vec<u8> {
        let mut field_buffer = Vec::new();
        for field in &self.fields {
            // Field Name
            field_buffer.extend_from_slice(&field.as_bytes());
            field_buffer.push(0);

            // Table OID (u32) or zero
            field_buffer.extend_from_slice(&0u32.to_be_bytes());

            // Column Index (u16) or zero
            field_buffer.extend_from_slice(&0u16.to_be_bytes());

            // Data Type OID (u32)
            field_buffer.extend_from_slice(&0u32.to_be_bytes());

            // Data Type Size (i16). Negative values denote variable length types.
            field_buffer.extend_from_slice(&0u16.to_be_bytes());

            // Type Modifier (u32). Type-dependent field.
            field_buffer.extend_from_slice(&0u32.to_be_bytes());

            // Format Code (u16). 0 = text (or unknown), 1 = binary
            field_buffer.extend_from_slice(&0u16.to_be_bytes());
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

impl Message for DataRow {
    fn encode(&self) -> Vec<u8> {
        let mut field_buffer = Vec::new();
        for field in &self.fields {
            match field {
                Some(value) => {
                    field_buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    field_buffer.extend_from_slice(&value.as_bytes());
                }
                None => {
                    // NULL or no value
                    field_buffer.extend_from_slice(&0xFFFFFFFFu32.to_be_bytes());
                }
            }
        }

        let mut buffer = Vec::new();
        buffer.push(b'D');
        buffer.extend_from_slice(&(field_buffer.len() as u32 + 4 + 2).to_be_bytes());
        buffer.extend_from_slice(&(self.fields.len() as u16).to_be_bytes());
        buffer.extend_from_slice(&field_buffer);

        buffer
    }
}

#[test]
fn test_empty_data_row() -> Result<(), Box<dyn Error>> {
    let data_row = DataRow::builder().build();

    let encoded = data_row.encode();
    assert_eq!(encoded.len(), 7);
    assert_eq!(
        encoded,
        vec![
            // message tag
            b'D', // length
            0x00, 0x00, 0x00, 6, // field count
            0x00, 0x00,
        ]
    );

    let mut cursor = Cursor::new(encoded);
    let decoded = BackendMessage::read_next_message(&mut cursor)?;
    assert_eq!(decoded, BackendMessage::DataRow(data_row));

    Ok(())
}

impl Message for CommandComplete {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'C');

        let mut tag_bytes: Vec<u8> = self.tag.as_bytes().to_vec();
        tag_bytes.push(0);
        let length: u32 = (4 + tag_bytes.len()) as u32;

        buffer.extend_from_slice(&length.to_be_bytes());
        buffer.extend_from_slice(&tag_bytes);
        buffer
    }
}

#[test]
fn test_empty_command_complete() -> Result<(), Box<dyn Error>> {
    let command_complete = CommandComplete::builder().build();

    let encoded = command_complete.encode();
    assert_eq!(encoded.len(), 6);
    assert_eq!(
        encoded,
        vec![
            // message tag
            b'C', // length
            0x00, 0x00, 0x00, 5, // tag
            0x00,
        ]
    );

    let mut cursor = Cursor::new(encoded);
    let decoded = BackendMessage::read_next_message(&mut cursor)?;
    assert_eq!(decoded, BackendMessage::CommandComplete(command_complete));

    Ok(())
}

#[test]
fn test_select1_command_complete() -> Result<(), Box<dyn Error>> {
    let command_complete = CommandComplete::builder().tag("SELECT 1").build();

    let encoded = command_complete.encode();
    assert_eq!(encoded.len(), 14);
    assert_eq!(
        encoded,
        vec![
            // message tag
            b'C', // length
            0x00, 0x00, 0x00, 13, // tag
            b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'1', 0x00,
        ]
    );

    let mut cursor = Cursor::new(encoded);
    let decoded = BackendMessage::read_next_message(&mut cursor)?;
    assert_eq!(decoded, BackendMessage::CommandComplete(command_complete));

    Ok(())
}

impl Message for BackendMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            BackendMessage::ReadyForQuery(ready_for_query) => ready_for_query.encode(),
            BackendMessage::RowDescription(row_description) => row_description.encode(),
            BackendMessage::DataRow(data_row) => data_row.encode(),
            BackendMessage::CommandComplete(command_complete) => command_complete.encode(),
            BackendMessage::Error { length } => {
                let mut buffer = Vec::new();
                buffer.push(b'E');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer
            }
        }
    }
}
