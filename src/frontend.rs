use std::{error::Error, io::Write, net::TcpStream};

use crate::messages::{frontend::FrontendMessage, startup::StartupRequest, Message};

#[derive(Debug)]
pub struct Frontend {
    stream: TcpStream,
}

impl Frontend {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn read_startup_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = StartupRequest>, Box<dyn Error>> {
        struct MessageIterator(TcpStream, bool);
        impl Iterator for MessageIterator {
            type Item = StartupRequest;

            fn next(&mut self) -> Option<Self::Item> {
                if self.1 {
                    return None;
                }

                match StartupRequest::read_next_message(&mut self.0) {
                    Ok(message) => {
                        match message {
                            StartupRequest::CancelRequest(_) => {
                                self.1 = true;
                                println!("cancel request");
                            },
                            StartupRequest::Startup(_) => {
                                self.1 = true;
                                println!("startup");
                            },
                            StartupRequest::SSLRequest(_) => {
                                self.1 = false;
                                println!("ssl request");
                            },
                        }
                        Some(message)
                    }
                    Err(err) => {
                        println!("error reading startup message: {err}");
                        None
                    }
                }
            }
        }

        Ok(MessageIterator(self.stream.try_clone()?, false))
    }

    pub fn read_messages(
        &mut self,
    ) -> Result<impl Iterator<Item = FrontendMessage>, Box<dyn Error>> {
        Ok(MessageIterator(self.stream.try_clone()?, false))
    }

    pub fn send_message(
        &mut self,
        message: impl Message + core::fmt::Debug,
    ) -> Result<(), Box<dyn Error>> {
        println!("Frontend send_message: {message:?}");
        self.stream.write_all(&message.encode())?;
        //self.stream.flush()?;
        Ok(())
    }
}

struct MessageIterator(TcpStream, bool);
impl Iterator for MessageIterator {
    type Item = FrontendMessage;
    fn next(&mut self) -> Option<FrontendMessage> {
        if self.1 {
            return None;
        }

        match FrontendMessage::read_next_message(&mut self.0) {
            Ok(FrontendMessage::Termination(termination)) => {
                self.1 = true;
                Some(FrontendMessage::Termination(termination))
            }
            Ok(message) => Some(message),
            Err(err) => {
                println!("error reading frontend message: {err}");
                None
            }
        }
    }
}
