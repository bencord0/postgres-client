use crate::messages::Message;
use core::fmt;
use std::str;

#[derive(Debug, Default, Clone, Copy)]
pub enum Authentication {
    #[default]
    Ok,
}

impl Message for Authentication {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'R');

        match self {
            Authentication::Ok => {
                let length: u32 = 8;
                let r#type: u32 = 0;

                buffer.extend_from_slice(&length.to_be_bytes());
                buffer.extend_from_slice(&r#type.to_be_bytes());
            }
        };

        buffer
    }
}

#[test]
fn test_authentication_ok() {
    let message = Authentication::Ok;
    let encoded = message.encode();
    assert_eq!(encoded, vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
}

#[derive(Debug)]
pub struct ParameterStatus {
    pub name: String,
    pub value: String,
}

impl Message for ParameterStatus {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'S');

        let length: u32 = 4 + self.name.len() as u32 + 1 + self.value.len() as u32 + 1;
        buffer.extend_from_slice(&length.to_be_bytes());

        buffer.extend_from_slice(self.name.as_bytes());
        buffer.push(0);

        buffer.extend_from_slice(self.value.as_bytes());
        buffer.push(0);

        buffer
    }
}

#[derive(Debug, Default)]
pub struct BackendKeyData {
    pub process_id: u32,
    pub secret_key: u32,
}

impl Message for BackendKeyData {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'K');

        let length: u32 = 12;
        buffer.extend_from_slice(&length.to_be_bytes());

        buffer.extend_from_slice(&self.process_id.to_be_bytes());
        buffer.extend_from_slice(&self.secret_key.to_be_bytes());

        buffer
    }
}

#[derive(Debug, Clone, Default)]
pub enum TransactionStatus {
    #[default]
    Unknown,
    Idle,
    //T
    //E
}

impl TransactionStatus {
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            b'I' => TransactionStatus::Idle,
            _ => {
                panic!(
                    "unknown transaction status: {}",
                    str::from_utf8(&[value]).unwrap()
                );
            }
        }
    }

    pub(crate) fn to_u8(&self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            _ => {
                panic!("unknown transaction status: {:?}", self);
            }
        }
    }
}

impl fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionStatus::Idle => write!(f, "Idle"),
            _ => {
                panic!("unknown transaction status: {:?}", self);
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ReadyForQuery {
    pub transaction_status: TransactionStatus,
}

impl Message for ReadyForQuery {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'Z');

        let length: u32 = 5;
        buffer.extend_from_slice(&length.to_be_bytes());

        buffer.push(self.transaction_status.to_u8());

        buffer
    }
}
