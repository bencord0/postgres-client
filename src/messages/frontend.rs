use std::{
    error::Error,
    io::{Cursor, Read},
    str,
};

use crate::{messages::Message, readers::*};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    SimpleQuery(SimpleQuery),
    Termination(Termination),
}

impl FrontendMessage {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let mut header: Vec<u8> = vec![0; 5];
        let bytes_read = stream.read(&mut header)?;
        if bytes_read != 5 {
            return Err("Failed to read header".into());
        }

        let r#type: u8 = header[0];
        let length: u32 = u32::from_be_bytes(header[1..5].try_into()?);
        let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);

        let message: FrontendMessage = match r#type {
            b'Q' => FrontendMessage::SimpleQuery(SimpleQuery::read_next_message(&mut buffer)?),
            b'X' => {
                assert_eq!(length, 4);
                FrontendMessage::Termination(Termination)
            }
            unknown_type => {
                return Err(format!(
                    "Unknown message type: {} ({unknown_type})",
                    str::from_utf8(&[unknown_type])?
                )
                .into());
            }
        };

        Ok(message)
    }
}

impl Message for FrontendMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            FrontendMessage::SimpleQuery(query) => query.encode(),
            FrontendMessage::Termination(terminationa) => terminationa.encode(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleQuery {
    query: String,
}

impl SimpleQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }

    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        Ok(SimpleQuery::new(read_string(stream)?))
    }
}

impl Message for SimpleQuery {
    fn encode(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        buffer.push(b'Q');
        // 4 bytes for length
        // 1 byte for null terminator
        buffer.extend_from_slice(&(self.query.len() as u32 + 4 + 1).to_be_bytes());
        buffer.extend_from_slice(&self.query.as_bytes());
        buffer.push(0);

        buffer
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Termination;

impl Message for Termination {
    fn encode(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        buffer.push(b'X');
        buffer.extend_from_slice(&4u32.to_be_bytes());

        buffer
    }
}
