use std::{error::Error, io::Read, str};

use crate::{messages::Message, readers::*, state::TransactionStatus};

#[derive(Debug, Clone)]
pub enum BackendMessage {
    ReadyForQuery {
        length: u32,
        transaction_status: TransactionStatus,
    },
    RowDescription {
        length: u32,
        fields: Vec<String>,
    },
    DataRow {
        length: u32,
        fields: Vec<Option<String>>,
    },
    CommandComplete {
        length: u32,
        tag: String,
    },
    Error {
        length: u32,
    },
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

        let message: BackendMessage = match r#type {
            b'Z' => BackendMessage::ReadyForQuery {
                length,
                transaction_status: TransactionStatus::from_u8(read_u8(stream)?),
            },
            b'T' => {
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

                BackendMessage::RowDescription {
                    length,
                    fields: fields,
                }
            }
            b'D' => {
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

                BackendMessage::DataRow { length, fields }
            }
            b'C' => BackendMessage::CommandComplete {
                length,
                tag: read_string(stream)?,
            },
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

impl Message for BackendMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            BackendMessage::ReadyForQuery {
                length,
                transaction_status,
            } => {
                let mut buffer = Vec::new();
                buffer.push(b'Z');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer.extend_from_slice(&[transaction_status.to_u8()]);
                buffer
            }
            BackendMessage::RowDescription { length, fields } => {
                let mut buffer = Vec::new();
                buffer.push(b'T');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer.extend_from_slice(&(fields.len() as u16).to_be_bytes());

                for field in fields {
                    buffer.extend_from_slice(&field.as_bytes());
                    buffer.push(0);
                    buffer.extend_from_slice(&0u32.to_be_bytes());
                    buffer.extend_from_slice(&0u16.to_be_bytes());
                    buffer.extend_from_slice(&0u32.to_be_bytes());
                    buffer.extend_from_slice(&0u16.to_be_bytes());
                    buffer.extend_from_slice(&0u32.to_be_bytes());
                    buffer.extend_from_slice(&0u16.to_be_bytes());
                }

                buffer
            }
            BackendMessage::DataRow { length, fields } => {
                let mut buffer = Vec::new();
                buffer.push(b'D');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer.extend_from_slice(&(fields.len() as u16).to_be_bytes());

                for field in fields {
                    match field {
                        Some(value) => {
                            buffer.extend_from_slice(&(value.len() as u32).to_be_bytes());
                            buffer.extend_from_slice(&value.as_bytes());
                        }
                        None => {
                            buffer.extend_from_slice(&0xFFFFFFFFu32.to_be_bytes());
                        }
                    }
                }

                buffer
            }
            BackendMessage::CommandComplete { length, tag } => {
                let mut buffer = Vec::new();
                buffer.push(b'C');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer.extend_from_slice(&tag.as_bytes());
                buffer.push(0);
                buffer
            }
            BackendMessage::Error { length } => {
                let mut buffer = Vec::new();
                buffer.push(b'E');
                buffer.extend_from_slice(&length.to_be_bytes());
                buffer
            }
        }
    }
}
