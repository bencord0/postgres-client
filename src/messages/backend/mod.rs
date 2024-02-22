use std::{
    error::Error,
    io::{Cursor, Read},
    str,
};

use crate::{messages::Message, readers::*};

mod ready_for_query;
mod row_description;
mod data_row;
mod empty_query_response;
mod no_data;
pub use ready_for_query::ReadyForQuery;
pub use row_description::RowDescription;
pub use data_row::DataRow;
pub use empty_query_response::EmptyQueryResponse;
pub use no_data::NoData;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    ReadyForQuery(ReadyForQuery),
    RowDescription(RowDescription),
    DataRow(DataRow),
    NoData(NoData),
    CommandComplete(CommandComplete),
    EmptyQueryResponse(EmptyQueryResponse),
    Error { length: u32 },
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
            b'Z' => BackendMessage::ReadyForQuery(ReadyForQuery::read_next_message(&mut buffer)?),
            b'T' => BackendMessage::RowDescription(RowDescription::read_next_message(&mut buffer)?),
            b'D' => BackendMessage::DataRow(DataRow::read_next_message(&mut buffer)?),
            b'n' => BackendMessage::NoData(NoData::read_next_message(&mut buffer)?),
            b'C' => {
                BackendMessage::CommandComplete(CommandComplete::read_next_message(&mut buffer)?)
            }
            b'I' => BackendMessage::EmptyQueryResponse(EmptyQueryResponse::read_next_message(&mut buffer)?),
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
            b'D',                // message tag
            0x00, 0x00, 0x00, 6, // length
            0x00, 0x00,          // field count
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
            BackendMessage::NoData(no_data) => no_data.encode(),
            BackendMessage::CommandComplete(command_complete) => command_complete.encode(),
            BackendMessage::EmptyQueryResponse(empty_query_response) => empty_query_response.encode(),
            BackendMessage::Error { length } => {
                let mut buffer = Vec::new();
                buffer.push(b'E');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer
            }
        }
    }
}
