use std::{
    error::Error,
    io::Write,
    net::TcpStream,
    time::Duration,
};

use rpsql::{
    messages::{
        frontend::{
            FrontendMessage,
            StartupMessage,
            SimpleQuery,
        },
        backend::{
            BackendMessage,
            TransactionStatus,
        },
    },
    state::{
        Authentication,
        KeyData,
    },
};

fn main() -> Result<(), Box<dyn Error>>{
    let mut pg = Pg::connect("127.0.0.1:5432")?;

    let query = SimpleQuery::new("SELECT * FROM apps LIMIT 10");
    pg.send_message(query)?;
    pg.wait_until_idle()?;

    Ok(())
}

#[derive(Debug, Default)]
struct Pg {
    connection: Option<TcpStream>,
    authentication: Option<Authentication>,
    parameters: std::collections::HashMap<String, String>,
    key_data: Option<KeyData>,

    // Query State
    row_description: Vec<String>,
}

impl Pg {
    fn connect(target: &str) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(target)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let mut startup_message = StartupMessage::new();
        startup_message.add_parameter("user", "bencord0");
        startup_message.add_parameter("database", "slingshot");
        startup_message.add_parameter("application_name", "rpsql");
        startup_message.add_parameter("client_encoding", "UTF8");

        let mut pg = Self {
            connection: Some(stream),
            ..Default::default()
        };

        pg.send_message(startup_message)?;
        pg.wait_until_idle()?;
        Ok(pg)
    }

    fn read_next_message(&mut self) -> Result<BackendMessage, Box<dyn Error>> {
        let Some(ref mut connection) = self.connection
        else {
            return Err("no connection".into());
        };

        let message = BackendMessage::read_next_message(connection)?;
        Ok(message)
    }

    fn send_message(&mut self, message: impl FrontendMessage + core::fmt::Debug) -> Result<(), Box<dyn Error>> {
        let Some(ref mut connection) = self.connection
        else {
            return Err("no connection".into());
        };

        println!("frontend: {message:?}");
        connection.write_all(&message.encode())?;

        Ok(())
    }

    fn wait_until_idle(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            let message = self.read_next_message()?;

            match message {
                BackendMessage::AuthenticationOk { authentication_type: 0, .. } => {
                    println!("authentication ok");
                    self.authentication = Some(Authentication::Ok);
                },

                BackendMessage::ParameterStatus { parameter_name, parameter_value, .. } => {
                    println!("parameter status: {} = {}", parameter_name, parameter_value);
                    self.parameters.insert(parameter_name, parameter_value);
                },

                BackendMessage::BackendKeyData { process_id, secret_key, .. } => {
                    println!("backend data: process_id = {process_id}");
                    self.key_data = Some(KeyData {
                        process_id,
                        secret_key,
                    });
                },

                BackendMessage::ReadyForQuery { transaction_status: TransactionStatus::Idle, .. } => {
                    println!("ready for query");
                    break;
                },

                BackendMessage::RowDescription { fields, .. } => {
                    println!("row description: {fields:?}");
                    self.row_description = fields;
                },

                BackendMessage::DataRow { fields, .. } => {
                    assert_eq!(fields.len(), self.row_description.len());
                    println!();
                    for (field, value) in self.row_description.iter().zip(fields) {
                        println!("  {} = {}", field, value.unwrap_or_else(|| "NULL".into()));
                    }
                },

                BackendMessage::CommandComplete { tag, .. } => {
                    println!("command complete: {}", tag);
                    self.row_description.clear();
                },

                _ => {
                    println!("unhandled message: {:?}", message);
                    break;
                },
            }
        }

        Ok(())
    }
}
