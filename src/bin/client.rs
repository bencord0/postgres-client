use std::{error::Error, net::TcpStream, time::Duration};

use rpsql::{
    messages::{
        backend::{BackendMessage, CommandComplete, DataRow, RowDescription},
        frontend::{SimpleQuery, Termination},
        ssl::{SSLRequest, SSLResponse},
        startup::{Startup, StartupResponse},
    },
    state::{Authentication, BackendKeyData, ParameterStatus, ReadyForQuery},
    Backend,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut pg = Pg::new();
    let mut backend = pg.connect("127.0.0.1:54321")?;

    let ssl_message = SSLRequest;
    backend.send_message(ssl_message)?;
    let SSLResponse::N = backend.read_ssl_message()? else {
        return Err("expected SSL answer".into());
    };

    let mut startup_message = Startup::new();
    startup_message.add_parameter("user", "bencord0");
    startup_message.add_parameter("database", "slingshot");
    startup_message.add_parameter("application_name", "rpsql-client");
    startup_message.add_parameter("client_encoding", "UTF8");
    backend.send_message(startup_message)?;

    for backend_startup_message in backend.read_startup_messages()? {
        match backend_startup_message {
            StartupResponse::Authentication(Authentication::Ok) => {
                println!("authentication ok");
                pg.authentication = Some(Authentication::Ok);
            }

            StartupResponse::ParameterStatus(ParameterStatus { name, value }) => {
                println!("parameter status: {name}, {value}");
            }

            StartupResponse::BackendKeyData(BackendKeyData {
                process_id,
                secret_key: _,
            }) => {
                println!("backend data: process_id = {process_id}");
            }

            StartupResponse::ReadyForQuery(ReadyForQuery { transaction_status }) => {
                println!("ready for query: {transaction_status}");
                break;
            }
        }
    }

    let query = SimpleQuery::new("SELECT * FROM apps LIMIT 10");
    backend.send_message(query)?;

    for message in backend.read_messages()? {
        match message {
            BackendMessage::RowDescription(RowDescription { fields }) => {
                println!("row description: {fields:?}");
                pg.row_description = fields;
            }

            BackendMessage::DataRow(DataRow { fields }) => {
                assert_eq!(fields.len(), pg.row_description.len());
                println!();
                for (field, value) in pg.row_description.iter().zip(fields) {
                    println!("  {} = {}", field, value.unwrap_or_else(|| "NULL".into()));
                }
            }

            BackendMessage::CommandComplete(CommandComplete { tag }) => {
                println!("command complete: {}", tag);
                pg.row_description.clear();
            }

            BackendMessage::ReadyForQuery { .. } => {
                println!("all done");
                break;
            }

            _ => {
                println!("unhandled message: {:?}", message);
                break;
            }
        }
    }

    let termination = Termination;
    backend.send_message(termination)?;

    Ok(())
}

#[derive(Debug, Default)]
struct Pg {
    authentication: Option<Authentication>,
    parameters: std::collections::HashMap<String, String>,
    key_data: Option<BackendKeyData>,

    // Query State
    row_description: Vec<String>,
}

impl Pg {
    fn new() -> Self {
        Self::default()
    }

    fn connect(&self, target: &str) -> Result<Backend, Box<dyn Error>> {
        let stream = TcpStream::connect(target)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let backend = Backend::new(stream);
        Ok(backend)
    }
}
