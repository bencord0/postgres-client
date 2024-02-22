use std::{
    error::Error,
    ops::DerefMut,
    sync::{Arc, atomic::{AtomicBool, Ordering}, Mutex},
};
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::BufReader,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

use crate::{
    messages::{
        backend::BackendMessage, ssl::SSLResponse, startup::StartupResponse, Message,
    },
};

#[derive(Debug)]
pub struct AsyncBackend {
    reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl AsyncBackend {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: Arc::new(Mutex::new(BufReader::new(reader))),
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub async fn send_message(
        &mut self,
        message: impl Message + std::fmt::Debug,
    ) -> Result<(), Box<dyn Error>> {
        println!("Backend send_message: {message:?}");
        let mut message = message.encode();

        loop {
            let writer = self.writer.lock().unwrap();
            writer.writable().await?;

            match writer.try_write(&message) {
                Ok(n) => {
                    message = (&message[n..]).to_vec();
                    if message.is_empty() {
                        break;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }

    pub async fn read_ssl_message(&mut self) -> Result<SSLResponse, Box<dyn Error>> {
        let mut reader = self.reader.lock().unwrap();
        match SSLResponse::read_next_message_async(reader.deref_mut()).await {
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
    ) -> impl Iterator<Item=NextStartupResponse> {
        struct MessageIterator {
            reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
            finished: Arc<AtomicBool>,
        }
        impl Iterator for MessageIterator {
            type Item = NextStartupResponse;

            fn next(&mut self) -> Option<Self::Item> {
                if self.finished.load(Ordering::Relaxed) {
                    return None;
                }

                Some(NextStartupResponse {
                    reader: self.reader.clone(),
                    finished: self.finished.clone(),
                })
            }
        }

        MessageIterator {
            reader: self.reader.clone(),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn read_messages(
        &mut self,
    ) -> impl Iterator<Item=NextBackendMessage> {
        struct MessageIterator {
            reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
            finished: Arc<AtomicBool>,
        }
        impl Iterator for MessageIterator {
            type Item = NextBackendMessage;

            fn next(&mut self) -> Option<Self::Item> {
                if self.finished.load(Ordering::Relaxed) {
                    return None;
                }

                Some(NextBackendMessage {
                    reader: self.reader.clone(),
                    finished: self.finished.clone(),
                })
            }
        }

        MessageIterator {
            reader: self.reader.clone(),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug)]
pub struct NextStartupResponse {
    reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
    finished: Arc<AtomicBool>,
}
impl Future for NextStartupResponse {
    type Output = Result<Option<StartupResponse>, Box<dyn Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finished.load(Ordering::Relaxed) {
            return Poll::Ready(Ok(None));
        }

        let mut reader = self.reader.lock().unwrap();
        let mut future = StartupResponse::read_next_message_async(&mut *reader);
        let x = match std::pin::pin!(future).poll(cx) {
            Poll::Ready(Ok(Some(item))) => {
                if let StartupResponse::ReadyForQuery(_) = item {
                    self.finished.store(true, Ordering::Relaxed);
                };
                Poll::Ready(Ok(Some(item)))
            },
            Poll::Ready(Ok(None)) => {
                self.finished.store(true, Ordering::Relaxed);
                Poll::Ready(Ok(None))
            },
            Poll::Ready(Err(err)) => {
                self.finished.store(true, Ordering::Relaxed);
                Poll::Ready(Err(err.into()))
            },
            Poll::Pending => {
                Poll::Pending
            },
        }; x
    }
}

#[derive(Debug)]
pub struct NextBackendMessage {
    reader: Arc<Mutex<BufReader<OwnedReadHalf>>>,
    finished: Arc<AtomicBool>,
}
impl Future for NextBackendMessage {
    type Output = Result<Option<BackendMessage>, Box<dyn Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finished.load(Ordering::Relaxed) {
            return Poll::Ready(Ok(None));
        }

        let mut reader = self.reader.lock().unwrap();
        let mut future = BackendMessage::read_next_message_async(&mut *reader);
        let x = match std::pin::pin!(future).poll(cx) {
            Poll::Ready(Ok(item)) => {
                if let BackendMessage::ReadyForQuery(_) = item {
                    self.finished.store(true, Ordering::Relaxed);
                };
                Poll::Ready(Ok(Some(item)))
            },
            Poll::Ready(Err(err)) => {
                self.finished.store(true, Ordering::Relaxed);
                Poll::Ready(Err(err.into()))
            },
            Poll::Pending => {
                Poll::Pending
            },
        }; x
    }
}