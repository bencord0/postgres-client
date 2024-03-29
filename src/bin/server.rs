use std::{error::Error, net::TcpListener};

use rpsql::{
    messages::{
        backend::{CommandComplete, DataRow, ReadyForQuery, RowDescription},
        frontend::FrontendMessage,
        ssl::SSLResponse,
        startup::StartupRequest,
    },
    state::{Authentication, TransactionStatus},
    Frontend,
};

fn main() -> Result<(), Box<dyn Error>> {
    let pg = Pg::bind("127.0.0.1:54321")?;
    println!("Listening on 127.0.0.1:54321");

    'connection: for mut frontend in pg.connections() {
        println!("New connection from frontend");

        for startup_request in frontend.read_startup_messages()? {
            match startup_request {
                StartupRequest::CancelRequest(_) => continue 'connection,
                StartupRequest::SSLRequest(_) => {
                    let ssl_response = SSLResponse::N;
                    frontend.send_message(ssl_response)?;
                    continue;
                }
                StartupRequest::Startup(_) => {
                    frontend.send_message(Authentication::Ok)?;

                    frontend.send_message(ReadyForQuery {
                        transaction_status: TransactionStatus::Idle,
                    })?;
                    break;
                }
            }
        }

        for message in frontend.read_messages()? {
            println!("Message from frontend: {:?}", message);

            match message {
                FrontendMessage::SimpleQuery(_query) => {
                    let row_description =
                        RowDescription::builder().string_field("greeting").build();
                    frontend.send_message(row_description)?;

                    let data_row = DataRow::builder().string_field("Hello, world!").build();
                    frontend.send_message(data_row)?;

                    let command_complete = CommandComplete::builder().tag("GREETING").build();
                    frontend.send_message(command_complete)?;

                    frontend.send_message(ReadyForQuery {
                        transaction_status: TransactionStatus::Idle,
                    })?;
                }
                FrontendMessage::Termination(_) => continue 'connection,
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Pg {
    listener: TcpListener,
}

impl Pg {
    fn bind(target: &str) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(target)?;
        Ok(Self { listener })
    }

    fn connections(&self) -> impl Iterator<Item = Frontend> + '_ {
        self.listener
            .incoming()
            .filter_map(Result::ok)
            .map(Frontend::new)
    }
}
