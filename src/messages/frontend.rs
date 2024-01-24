pub trait FrontendMessage {
    fn encode(&self) -> Vec<u8>;
}

#[derive(Debug)]
pub struct StartupMessage {
    protocol_major_version: u16,
    protocol_minor_version: u16,
    parameters: Vec<(String, String)>,
}

impl Default for StartupMessage {
    fn default() -> Self {
        Self {
            protocol_major_version: 3,
            protocol_minor_version: 0,
            parameters: vec![],
        }
    }
}

impl StartupMessage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_parameter(&mut self, key: &str, value: &str) {
        self.parameters.push((key.to_string(), value.to_string()));
    }
}

impl FrontendMessage for StartupMessage {
    fn encode(&self) -> Vec<u8> {
        let mut parameter_buffer: Vec<u8> = vec![];
        for (key, value) in &self.parameters {
            parameter_buffer.extend_from_slice(key.as_bytes());
            parameter_buffer.push(0);

            parameter_buffer.extend_from_slice(value.as_bytes());
            parameter_buffer.push(0);
        }

        // 4 bytes for length
        // 2 bytes for protocol major version
        // 2 bytes for protocol minor version
        // 1 byte for null terminator
        let length: u32 = 4 + 4 + parameter_buffer.len() as u32 + 1;

        let mut buffer: Vec<u8> = vec![];

        buffer.extend_from_slice(&length.to_be_bytes());
        buffer.extend_from_slice(&self.protocol_major_version.to_be_bytes());
        buffer.extend_from_slice(&self.protocol_minor_version.to_be_bytes());
        buffer.extend_from_slice(&parameter_buffer);
        buffer.push(0);

        buffer
    }
}

#[derive(Debug)]
pub struct SimpleQuery {
    query: String,
}

impl SimpleQuery {
    pub fn new(query: &str) -> Self {
        Self {
            query: query.into(),
        }
    }
}

impl FrontendMessage for SimpleQuery {
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
