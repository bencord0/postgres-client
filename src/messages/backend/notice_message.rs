use crate::{messages::Message, readers::*};
use core::fmt;
use std::{error::Error, io::Read};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeMessage {
    pub severity: Severity,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Severity {
    #[default]
    Warning,
    Notice,
    Debug,
    Info,
    Log,
    Localized(String),
}

impl NoticeMessage {
    pub fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let mut builder = NoticeMessage::builder();
        loop {
            match read_u8(stream)? {
                b'S' => {
                    let severity = Severity::read_next_message(stream)?;
                    builder = builder.severity(severity);
                }
                b'V' => {
                    let severity = match read_string(stream)?.as_str() {
                        "WARNING" => Severity::Warning,
                        "NOTICE" => Severity::Notice,
                        "DEBUG" => Severity::Debug,
                        "INFO" => Severity::Info,
                        "LOG" => Severity::Log,
                        other => Severity::Localized(other.to_string()),
                    };
                    builder = builder.severity(severity);
                }
                b'C' => {
                    let code = read_string(stream)?;
                    builder = builder.code(code);
                }
                b'M' => {
                    let message = read_string(stream)?;
                    builder = builder.message(message);
                }
                b'F' => {
                    let _file_name = read_string(stream)?;
                }
                b'L' => {
                    let _line_no = read_string(stream)?;
                }
                b'R' => {
                    let _routine = read_string(stream)?;
                }
                0 => break,

                field_type => {
                    let field_type = String::from_utf8(vec![field_type])?;
                    let field_value = read_string(stream)?;
                    eprintln!("Unknown field type: {field_type}");
                    eprintln!("  : {field_value}");

                    continue;
                }
            }
        }

        Ok(builder.build()?)
    }

    pub fn builder() -> NoticeMessageBuilder {
        NoticeMessageBuilder::new()
    }
}

impl Severity {
    fn read_next_message(stream: &mut impl Read) -> Result<Self, Box<dyn Error>> {
        let value = read_string(stream)?;
        Ok(match value.as_str() {
            "WARNING" => Severity::Warning,
            "NOTICE" => Severity::Notice,
            "DEBUG" => Severity::Debug,
            "INFO" => Severity::Info,
            "LOG" => Severity::Log,
            other => Severity::Localized(other.to_string()),
        })
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Warning => write!(f, "WARNING"),
            Severity::Notice => write!(f, "NOTICE"),
            Severity::Debug => write!(f, "DEBUG"),
            Severity::Info => write!(f, "INFO"),
            Severity::Log => write!(f, "LOG"),
            Severity::Localized(value) => write!(f, "{}", value),
        }
    }
}

impl Message for NoticeMessage {
    fn encode(&self) -> Vec<u8> {
        let mut inner = Vec::new();

        // Severity
        inner.extend_from_slice(&self.severity.encode());

        // Code
        inner.push(b'C');
        inner.extend_from_slice(self.code.as_bytes());
        inner.push(0);

        // Message
        inner.push(b'M');
        inner.extend_from_slice(self.message.as_bytes());
        inner.push(0);

        let mut buffer = Vec::new();
        buffer.push(b'N');

        buffer.extend_from_slice(&(inner.len() as u32 + 4 + 1).to_be_bytes());
        buffer.extend_from_slice(&inner);

        // terminator
        buffer.push(0);
        buffer
    }
}

impl Message for Severity {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(b'S');

        match self {
            Severity::Warning => buffer.extend_from_slice(b"WARNING"),
            Severity::Notice => buffer.extend_from_slice(b"NOTICE"),
            Severity::Debug => buffer.extend_from_slice(b"DEBUG"),
            Severity::Info => buffer.extend_from_slice(b"INFO"),
            Severity::Log => buffer.extend_from_slice(b"LOG"),
            Severity::Localized(value) => buffer.extend_from_slice(value.as_bytes()),
        }
        buffer.push(0);
        buffer
    }
}

pub struct NoticeMessageBuilder {
    severity: Option<Severity>,
    code: Option<String>,
    message: Option<String>,
}

impl NoticeMessageBuilder {
    pub fn new() -> Self {
        Self {
            severity: None,
            code: None,
            message: None,
        }
    }

    pub fn severity(mut self, severity: Severity) -> Self {
        self.severity = Some(severity);
        self
    }

    pub fn code(mut self, code: String) -> Self {
        self.code = Some(code);
        self
    }

    pub fn message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }

    pub fn build(self) -> Result<NoticeMessage, Box<dyn Error>> {
        let severity = self.severity.unwrap_or_default(); //.ok_or("Severity is required")?;
        let code = self.code.unwrap_or_default(); //ok_or("Code is required")?;
        let message = self.message.unwrap_or_default(); //ok_or("Message is required")?;

        Ok(NoticeMessage {
            severity,
            code,
            message,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::backend::BackendMessage;
    use std::io::Cursor;

    //#[test]
    //fn test_notice_message() -> Result<(), Box<dyn Error>> {
    //    let notice_message = NoticeMessage::builder()
    //        .severity(Severity::Warning)
    //        .code("C25P01".to_string())
    //        .message("There is no transaction in progress".to_string())
    //        .build()?;

    //    let encoded = notice_message.encode();
    //    assert_eq!(encoded.len(), 103);
    //    assert_eq!(
    //        encoded,
    //        vec![
    //            // message tag
    //            b'N',

    //            // length
    //            0x00, 0x00, 0x00, 102,

    //            // ...
    //            0x53, 0x57, 0x41, 0x52, 0x4e, 0x49, 0x4e, 0x47, 0x0, 0x56, 0x57, 0x41, 0x52, 0x4e, 0x49, 0x4e, 0x47, 0x0, 0x43, 0x32, 0x35, 0x50, 0x30, 0x31, 0x0, 0x4d, 0x74, 0x68, 0x65, 0x72, 0x65, 0x20, 0x69, 0x73, 0x20, 0x6e, 0x6f, 0x20, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x69, 0x6e, 0x20, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x65, 0x73, 0x73, 0x0, 0x46, 0x78, 0x61, 0x63, 0x74, 0x2e, 0x63, 0x0, 0x4c, 0x33, 0x39, 0x37, 0x36, 0x0, 0x52, 0x45, 0x6e, 0x64, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x0, 0x0,
    //        ]
    //    );

    //    let mut cursor = Cursor::new(encoded);
    //    let decoded = BackendMessage::read_next_message(&mut cursor)?;
    //    assert_eq!(decoded, BackendMessage::NoticeMessage(notice_message));

    //    Ok(())
    //}

    #[test]
    fn test_empty_notice_message() -> Result<(), Box<dyn Error>> {
        let notice_message = NoticeMessage::builder()
            //.severity(Severity::Warning)
            //.code("25P01".to_string())
            //.message("There is no transaction in progress".to_string())
            .build()?;

        let encoded = notice_message.encode();
        assert_eq!(encoded.len(), 19);
        assert_eq!(
            encoded,
            vec![
                // message tag
                b'N',

                // length
                0x00, 0x00, 0x00, 18,

                // severity
                b'S', b'W', b'A', b'R', b'N', b'I', b'N', b'G', 0,

                // code
                b'C', 0,
                //b'2', b'5', b'P', b'0', b'1', 0,

                // message
                b'M', 0,
                //b'M', b'T', b'h', b'e', b'r', b'e', b' ', b'i', b's', b' ', b'n', b'o', b' ', b't', b'r', b'a', b'n', b's', b'a', b'c', b't', b'i', b'o', b'n', b' ', b'i', b'n', b' ', b'p', b'r', b'o', b'g', b'r', b'e', b's', b's', 0,

                // terminator
                0x00,
            ]
        );

        let mut cursor = Cursor::new(encoded);
        let decoded = BackendMessage::read_next_message(&mut cursor)
            .expect("Backend read_next_message");
        assert_eq!(decoded, BackendMessage::NoticeMessage(notice_message));

        Ok(())
    }

}
