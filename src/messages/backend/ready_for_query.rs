use std::{
    error::Error,
    io::Read,
};

use crate::{messages::Message, readers::*, state::TransactionStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyForQuery {
    pub transaction_status: TransactionStatus,
}

impl ReadyForQuery {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let transaction_status = TransactionStatus::from_u8(read_u8(stream)?);

        Ok(Self { transaction_status })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::messages::backend::BackendMessage;

    #[test]
    fn test_ready_for_query() -> Result<(), Box<dyn Error>> {
        let ready = ReadyForQuery {
            transaction_status: TransactionStatus::Idle,
        };

        let encoded = ready.encode();
        assert_eq!(encoded.len(), 6);
        assert_eq!(
            encoded,
            vec![
                // message tag
                b'Z',

                // length
                0x00, 0x00, 0x00, 5,

                // transaction state
                b'I',
            ]);

        let mut cursor = Cursor::new(encoded);
        let decoded = BackendMessage::read_next_message(&mut cursor)?;
        assert_eq!(decoded, BackendMessage::ReadyForQuery(ready));

        Ok(())
    }
}
