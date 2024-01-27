use std::{error::Error, io::Write, net::TcpStream};

use crate::messages::{
    backend::BackendMessage, ssl::SSLResponse, startup::StartupResponse, Message,
};

#[derive(Debug)]
pub struct Backend {
    stream: TcpStream,
}

impl Backend {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn send_message(
        &mut self,
        message: impl Message + std::fmt::Debug,
    ) -> Result<(), Box<dyn Error>> {
        println!("Backend send_message: {message:?}");
        self.stream.write_all(&message.encode())?;
        //self.stream.flush()?;
        Ok(())
    }

    pub fn read_ssl_message(&mut self) -> Result<SSLResponse, Box<dyn Error>> {
        match SSLResponse::read_next_message(&mut self.stream) {
            Ok(message) => {
                println!("Backend read_ssl_message: {message:?}");
                Ok(message)
            }
            Err(err) => {
                println!("error reading backend message: {err}");
                Err(err.into())
            }
        }
    }

    pub fn read_startup_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = StartupResponse>, Box<dyn Error>> {
        struct MessageIterator {
            stream: TcpStream,
            finished: bool,
        }
        impl Iterator for MessageIterator {
            type Item = StartupResponse;

            fn next(&mut self) -> Option<Self::Item> {
                if self.finished {
                    return None;
                }

                match Self::Item::read_next_message(&mut self.stream) {
                    Ok(Some(StartupResponse::ReadyForQuery(message))) => {
                        self.finished = true;
                        println!("Backend read_startup_messages final");
                        Some(StartupResponse::ReadyForQuery(message))
                    }
                    Ok(Some(message)) => Some(message),
                    Ok(None) => None,
                    Err(err) => {
                        println!("Backend read_startup_messages: {err}");
                        None
                    }
                }
            }
        }

        Ok(MessageIterator {
            stream: self.stream.try_clone()?,
            finished: false,
        })
    }

    pub fn read_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = BackendMessage>, Box<dyn Error>> {
        struct MessageIterator {
            stream: TcpStream,
            finished: bool,
        }
        impl Iterator for MessageIterator {
            type Item = BackendMessage;

            fn next(&mut self) -> Option<Self::Item> {
                if self.finished {
                    return None;
                }

                match BackendMessage::read_next_message(&mut self.stream) {
                    Ok(message) => {
                        if let BackendMessage::ReadyForQuery { .. } = message {
                            self.finished = true;
                        }
                        Some(message)
                    }
                    Err(err) => {
                        println!("error reading backend message: {err}");
                        None
                    }
                }
            }
        }

        Ok(MessageIterator {
            stream: self.stream.try_clone()?,
            finished: false,
        })
    }
}
