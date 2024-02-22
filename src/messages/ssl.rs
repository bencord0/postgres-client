use std::{
    error::Error,
    io::{Cursor, Read},
};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

use crate::{messages::Message, readers::*};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSLRequest;

impl SSLRequest {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let length = read_u32(stream)?;
        let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);

        let protocol_major_version = read_u16(&mut buffer)?;
        let protocol_minor_version = read_u16(&mut buffer)?;
        assert_eq!(protocol_major_version, 1234);
        assert_eq!(protocol_minor_version, 5679);
        Ok(SSLRequest)
    }

    pub async fn read_next_message_async<R: AsyncRead + Unpin>(stream: &mut BufReader<R>) -> Result<Self, Box<dyn Error>> {
        let length = stream.read_u32().await?;
        let mut buffer = Cursor::new(read_bytes_async(length as usize - 4, stream).await?);

        let protocol_major_version = read_u16(&mut buffer)?;
        let protocol_minor_version = read_u16(&mut buffer)?;
        assert_eq!(protocol_major_version, 1234);
        assert_eq!(protocol_minor_version, 5679);
        Ok(SSLRequest)
    }
}

impl Message for SSLRequest {
    fn encode(&self) -> Vec<u8> {
        let length: u32 = 8;
        let protocol_major_version: u16 = 1234;
        let protocol_minor_version: u16 = 5679;

        let mut buffer = vec![];
        buffer.extend_from_slice(&length.to_be_bytes());
        buffer.extend_from_slice(&protocol_major_version.to_be_bytes());
        buffer.extend_from_slice(&protocol_minor_version.to_be_bytes());

        buffer
    }
}

#[test]
fn test_ssl_request() -> Result<(), Box<dyn Error>> {
    let ssl_request = SSLRequest;
    let encoded = ssl_request.encode();
    assert_eq!(encoded.len(), 8);
    assert_eq!(encoded, vec![0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]);

    let mut cursor = Cursor::new(encoded);
    let decoded = SSLRequest::read_next_message(&mut cursor)?;
    assert_eq!(decoded, ssl_request);

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SSLResponse {
    S,
    N,
}

impl Message for SSLResponse {
    fn encode(&self) -> Vec<u8> {
        match self {
            SSLResponse::S => vec![b'S'],
            SSLResponse::N => vec![b'N'],
        }
    }
}

impl SSLResponse {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let message_type = read_u8(stream)?;
        match message_type {
            b'S' => Ok(SSLResponse::S),
            b'N' => Ok(SSLResponse::N),
            _ => Err("Unknown ssl response type".into()),
        }
    }

    pub async fn read_next_message_async(stream: &mut (impl AsyncReadExt + Unpin)) -> Result<Self, Box<dyn Error>> {

        let message_type = read_u8_async(stream).await?;
        match message_type {
            b'S' => Ok(SSLResponse::S),
            b'N' => Ok(SSLResponse::N),
            _ => Err("Unknown ssl response type".into()),
        }
    }
}

#[test]
fn test_ssl_response_s() -> Result<(), Box<dyn Error>> {
    let ssl_response = SSLResponse::S;
    let encoded = ssl_response.encode();
    assert_eq!(encoded.len(), 1);
    assert_eq!(encoded, vec![b'S']);

    let mut cursor = Cursor::new(encoded);
    let decoded = SSLResponse::read_next_message(&mut cursor)?;
    assert_eq!(decoded, ssl_response);

    Ok(())
}

#[test]
fn test_ssl_response_n() -> Result<(), Box<dyn Error>> {
    let ssl_response = SSLResponse::N;
    let encoded = ssl_response.encode();
    assert_eq!(encoded.len(), 1);
    assert_eq!(encoded, vec![b'N']);

    let mut cursor = Cursor::new(encoded);
    let decoded = SSLResponse::read_next_message(&mut cursor)?;
    assert_eq!(decoded, ssl_response);

    Ok(())
}

#[derive(Debug, Clone)]
pub enum SSLMessage {
    SSLRequest(SSLRequest),
    SSLResponse(SSLResponse),
}

impl SSLMessage {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let message_type = read_u8(stream)?;
        match message_type {
            b'S' => Ok(SSLMessage::SSLResponse(SSLResponse::S)),
            b'N' => Ok(SSLMessage::SSLResponse(SSLResponse::N)),
            0 => {
                let bytes = [0, read_u8(stream)?, read_u8(stream)?, read_u8(stream)?];
                let length: u32 = u32::from_be_bytes(bytes);
                let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);

                let protocol_major_version = read_u16(&mut buffer)?;
                let protocol_minor_version = read_u16(&mut buffer)?;
                assert_eq!(protocol_major_version, 1234);
                assert_eq!(protocol_minor_version, 5679);
                Ok(SSLMessage::SSLRequest(SSLRequest))
            }
            _ => Err("Unknown ssl message type".into()),
        }
    }
}
