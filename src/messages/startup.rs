use crate::{
    messages::{ssl::SSLRequest, Message},
    readers::*,
    state::{Authentication, BackendKeyData, ParameterStatus, ReadyForQuery},
};
use std::{
    error::Error,
    io::{Cursor, Read},
    str,
};
use tokio::io::{AsyncRead, BufReader};

#[derive(Debug, Clone)]
pub enum StartupRequest {
    SSLRequest(SSLRequest),
    Startup(Startup),
    CancelRequest(CancelRequest),
}

impl StartupRequest {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let length = read_u32(stream)? as usize;
        let protocol_major_version = read_u16(stream)?;
        let protocol_minor_version = read_u16(stream)?;

        let mut buffer = Cursor::new(read_bytes(length - 8, stream)?);
        match (length, protocol_major_version, protocol_minor_version) {
            (8, 1234, 5679) => Ok(Self::SSLRequest(SSLRequest)),
            (16, 1234, 5678) => {
                let process_id = read_u32(&mut buffer)?;
                let secret_key = read_u32(&mut buffer)?;
                Ok(Self::CancelRequest(CancelRequest {
                    process_id,
                    secret_key,
                }))
            },
            (_, 3, 0) => {
                let mut startup = Startup::new();

                loop {
                    let key = read_string(&mut buffer)?;
                    if key.is_empty() {
                        break;
                    }

                    let value = read_string(&mut buffer)?;
                    startup.add_parameter(&key, &value);
                }
                Ok(Self::Startup(startup))
            }
            (_, _, _) => panic!(
                "Unsupported protocol version: {protocol_major_version}.{protocol_minor_version}"
            ),
        }
    }
}

#[derive(Debug)]
pub enum StartupResponse {
    Authentication(Authentication),
    ParameterStatus(ParameterStatus),
    BackendKeyData(BackendKeyData),
    ReadyForQuery(ReadyForQuery),
}

impl StartupResponse {
    pub async fn read_next_message_async<R: AsyncRead + Unpin>(stream: &mut BufReader<R>) -> Result<Option<Self>, Box<dyn Error>> {
        let r#type = read_u8_async(stream).await?;

        let length = read_u32_async(stream).await? as usize;
        let mut buffer = Cursor::new(read_bytes_async(length - 4, stream).await?);

        let message = match r#type {
            b'R' => Some(Self::Authentication(Authentication::read_next_message(&mut buffer)?)),
            b'S' => Some(Self::ParameterStatus(ParameterStatus::read_next_message(&mut buffer)?)),
            b'K' => Some(Self::BackendKeyData(BackendKeyData::read_next_message(&mut buffer)?)),
            b'Z' => Some(Self::ReadyForQuery(ReadyForQuery::read_next_message(&mut buffer)?)),
            _ => {
                eprintln!("unsupported message type: {}", str::from_utf8(&[r#type])?);
                eprintln!("startup response length: {}", length);
                match str::from_utf8(buffer.get_ref()) {
                    Ok(s) => eprintln!("buffer: {s}"),
                    Err(_) => {},
                };
                return Err("unsupported message type".into());
            }
        };

        Ok(message)
    }

    pub fn read_next_message(stream: &mut impl Read) -> Result<Option<Self>, Box<dyn Error>> {
        let r#type = read_u8(stream)?;

        let length = read_u32(stream)? as usize;
        let mut buffer = Cursor::new(read_bytes(length - 4, stream)?);

        let message = match r#type {
            b'R' =>  Some(Self::Authentication(Authentication::read_next_message(&mut buffer)?)),
            b'S' => Some(Self::ParameterStatus(ParameterStatus::read_next_message(&mut buffer)?)),
            b'K' => Some(Self::BackendKeyData(BackendKeyData::read_next_message(&mut buffer)?)),
            b'Z' => Some(Self::ReadyForQuery(ReadyForQuery::read_next_message(&mut buffer)?)),
            _ => {
                eprintln!("unsupported message type: {}", str::from_utf8(&[r#type])?);
                eprintln!("startup response length: {}", length);
                match str::from_utf8(buffer.get_ref()) {
                    Ok(s) => eprintln!("buffer: {s}"),
                    Err(_) => {},
                };
                None
            }
         };

        Ok(message)
    }
}

impl Message for StartupRequest {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::SSLRequest(ssl_request) => ssl_request.encode(),
            Self::Startup(startup) => startup.encode(),
            Self::CancelRequest(cancel_request) => cancel_request.encode(),
        }
    }
}

impl Message for StartupResponse {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Authentication(authentication) => authentication.encode(),
            Self::ParameterStatus(parameter_status) => parameter_status.encode(),
            Self::BackendKeyData(backend_key_data) => backend_key_data.encode(),
            Self::ReadyForQuery(ready_for_query) => ready_for_query.encode(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Startup {
    length: u32,
    pub protocol_major_version: u16,
    pub protocol_minor_version: u16,
    pub parameters: Vec<(String, String)>,
}

impl Default for Startup {
    fn default() -> Self {
        Self {
            // 4 bytes for length
            // 2 bytes for protocol major version
            // 2 bytes for protocol minor version
            // 1 byte for null terminator
            length: 4 + 4 + 1,
            protocol_major_version: 3,
            protocol_minor_version: 0,
            parameters: vec![],
        }
    }
}

impl Startup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_parameter(&mut self, key: &str, value: &str) {
        self.parameters.push((key.to_string(), value.to_string()));
        self.length += key.len() as u32 + 1;
        self.length += value.len() as u32 + 1;
    }

    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let length = read_u32(stream)? as usize;
        let protocol_major_version = read_u16(stream)?;
        let protocol_minor_version = read_u16(stream)?;

        assert_eq!(protocol_major_version, 3);
        assert_eq!(protocol_minor_version, 0);

        let mut startup = Startup::new();
        let mut buffer = Cursor::new(read_bytes(length - 8, stream)?);
        loop {
            let key = read_string(&mut buffer)?;
            if key.is_empty() {
                break;
            }

            let value = read_string(&mut buffer)?;
            startup.add_parameter(&key, &value);
        }
        Ok(startup)
    }
}

impl Message for Startup {
    fn encode(&self) -> Vec<u8> {
        let mut parameter_buffer: Vec<u8> = vec![];
        for (key, value) in &self.parameters {
            parameter_buffer.extend_from_slice(key.as_bytes());
            parameter_buffer.push(0);

            parameter_buffer.extend_from_slice(value.as_bytes());
            parameter_buffer.push(0);
        }

        let mut buffer: Vec<u8> = vec![];

        buffer.extend_from_slice(&self.length.to_be_bytes());
        buffer.extend_from_slice(&self.protocol_major_version.to_be_bytes());
        buffer.extend_from_slice(&self.protocol_minor_version.to_be_bytes());
        buffer.extend_from_slice(&parameter_buffer);
        buffer.push(0);

        buffer
    }
}

#[derive(Debug, Clone)]
pub struct CancelRequest {
    pub process_id: u32,
    pub secret_key: u32,
}

impl Message for CancelRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![];

        buffer.extend_from_slice(&16u32.to_be_bytes());
        buffer.extend_from_slice(&1234u16.to_be_bytes());
        buffer.extend_from_slice(&5678u16.to_be_bytes());
        buffer.extend_from_slice(&self.process_id.to_be_bytes());
        buffer.extend_from_slice(&self.secret_key.to_be_bytes());

        buffer
    }
}
