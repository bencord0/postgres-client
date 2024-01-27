pub mod backend;
pub mod frontend;
pub mod ssl;
pub mod startup;

pub trait Message {
    fn encode(&self) -> Vec<u8>;
}
