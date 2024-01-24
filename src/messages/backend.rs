use std::{
    error::Error,
    io::{Cursor, Read},
    str,
};

#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk { length: u32, authentication_type: u32 },
    ParameterStatus { length: u32, parameter_name: String, parameter_value: String },
    BackendKeyData { length: u32, process_id: u32, secret_key: u32 },
    ReadyForQuery { length: u32, transaction_status: TransactionStatus },
    RowDescription { length: u32, fields: Vec<String> },
    DataRow { length: u32, fields: Vec<Option<String>> },
    CommandComplete { length: u32, tag: String },
    Error { length: u32 },
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
            b'R' => {
                let buffer = read_bytes(length as usize - 4, stream)?;
                BackendMessage::AuthenticationOk {
                    length,
                    authentication_type: read_u32(&mut &*buffer)?,
                }
            },
            b'S' => {
                let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);
                let parameter_name = read_string(&mut buffer)?;
                let parameter_value = read_string(&mut buffer)?;
                BackendMessage::ParameterStatus {
                    length,
                    parameter_name,
                    parameter_value,
                }
            },
            b'K' => {
                let mut buffer = Cursor::new(read_bytes(length as usize - 4, stream)?);
                let process_id = read_u32(&mut buffer)?;
                let secret_key = read_u32(&mut buffer)?;
                BackendMessage::BackendKeyData {
                    length,
                    process_id,
                    secret_key,
                }
            },
            b'Z' => {
                BackendMessage::ReadyForQuery {
                    length,
                    transaction_status: TransactionStatus::from_u8(read_u8(stream)?),
                }
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
            },
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

                BackendMessage::DataRow {
                    length,
                    fields,
                }
            },
            b'C' => {
                BackendMessage::CommandComplete {
                    length,
                    tag: read_string(stream)?,
                }
            },
            b'E' => {
                let _ = read_bytes(length as usize - 4, stream)?;
                BackendMessage::Error {
                    length,
                }
            },
            _ => {
                return Err(format!("unhandled message type: {:?}", str::from_utf8(&[r#type])?).into());
            }
        };

        Ok(message)
    }
}

#[derive(Debug)]
pub enum TransactionStatus {
    Unknown,
    Idle,
}

impl TransactionStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            b'I' => TransactionStatus::Idle,
            _ => {
                panic!("unknown transaction status: {}", str::from_utf8(&[value]).unwrap());
            }
        }
    }
}

fn read_u8(reader: &mut impl Read) -> Result<u8, Box<dyn Error>> {
    let mut buffer: [u8; 1] = [0; 1];
    reader.read_exact(&mut buffer)?;
    Ok(buffer[0])
}

fn read_u16(reader: &mut impl Read) -> Result<u16, Box<dyn Error>> {
    let mut buffer: [u8; 2] = [0; 2];
    reader.read_exact(&mut buffer)?;
    Ok(u16::from_be_bytes(buffer))
}

fn read_u32(reader: &mut impl Read) -> Result<u32, Box<dyn Error>> {
    let mut buffer: [u8; 4] = [0; 4];
    reader.read_exact(&mut buffer)?;
    Ok(u32::from_be_bytes(buffer))
}

fn read_bytes(length: usize, reader: &mut impl Read) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![0; length];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn read_string(reader: &mut impl Read) -> Result<String, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![];
    loop {
        let mut byte: [u8; 1] = [0; 1];
        reader.read_exact(&mut byte)?;
        if byte[0] == 0 {
            break;
        }
        buffer.push(byte[0]);
    }
    Ok(String::from_utf8(buffer)?)
}
