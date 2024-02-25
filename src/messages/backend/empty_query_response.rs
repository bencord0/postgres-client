use crate::messages::Message;
use std::{error::Error, io::Read};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyQueryResponse;

impl EmptyQueryResponse {
    pub fn read_next_message(_stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        Ok(Self)
    }
}

impl Message for EmptyQueryResponse {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'I');
        buffer.extend_from_slice(&4u32.to_be_bytes());
        buffer
    }
}
