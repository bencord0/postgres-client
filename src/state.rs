#[derive(Debug, Default)]
pub enum Authentication {
    #[default]
    Ok,
}

#[derive(Debug, Default)]
pub struct KeyData {
    pub process_id: u32,
    pub secret_key: u32,
}
