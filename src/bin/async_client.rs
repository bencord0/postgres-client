use std::{
    collections::HashMap,
    error::Error,
};
use tokio::net::TcpStream;
use clap::Parser;
use rpsql::{
    AsyncBackend as Backend,
    messages::startup::{Startup, StartupResponse},
    messages::frontend::{SimpleQuery, Termination},
    messages::backend::{BackendMessage, RowDescription},
    state::{Authentication, ParameterStatus, BackendKeyData, ReadyForQuery, TransactionStatus},
};

#[derive(Debug, Parser)]
#[command(author, version)]
struct Args {
    #[clap(long, default_value = "127.0.0.1")]
    host: String,

    #[clap(short, long, default_value = "5432")]
    port: u16,

    #[clap(short, long)]
    user: String,

    #[clap(short, long)]
    database: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let args = Args::parse();

    let mut pg = Pg::new();

    let mut backend = pg.connect(&args.host, args.port).await?;

    let mut startup_message = Startup::new();
    startup_message.add_parameter("user", &args.user);
    startup_message.add_parameter("database", &args.database);
    startup_message.add_parameter("client_encoding", "UTF8");
    startup_message.add_parameter("application_name", "pg-async");
    backend.send_message(startup_message).await?;

    do_startup(&mut pg, &mut backend).await?;
    let mut prompt = rustyline::DefaultEditor::new()?;

    loop {
        match prompt.readline(pg.prompt_prefix.as_str()) {
            Ok(line) => {
                let query = SimpleQuery::new(line);
                do_query(&mut pg, &mut backend, query).await?;
            }
            Err(err) => {
                eprintln!("EOF: {err}");
                break
            }
        }
    }

    let termination = Termination;
    backend.send_message(termination).await?;

    Ok(())
}

#[derive(Debug, Default)]
struct Pg {
    authentication: Option<Authentication>,
    parameters: HashMap<String, String>,
    key_data: Option<BackendKeyData>,

    // Query State
    row_description: Option<RowDescription>,

    // Prompt State
    prompt_prefix: String,
}

impl Pg {
    fn new() -> Self {
        Self::default()
    }

    async fn connect(&mut self, host: &str, port: u16) -> Result<Backend, Box<dyn Error>> {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let backend = Backend::new(stream);
        Ok(backend)
    }
}

async fn do_startup(pg: &mut Pg, backend: &mut Backend) -> Result<(), Box<dyn Error>> {
    let mut startup_messages = backend.read_startup_messages();
    while let Some(startup_message) = startup_messages.next() {
        let startup_message = startup_message.await?.ok_or("unexpected EOF")?;
        println!("{:?}", startup_message);

        match startup_message {
            StartupResponse::Authentication(auth) => {
                pg.authentication = Some(auth);
            }

            StartupResponse::ParameterStatus(ParameterStatus {name, value}) => {
                pg.parameters.insert(name, value);
            }

            StartupResponse::BackendKeyData(key_data) => {
                pg.key_data = Some(key_data);
            }

            StartupResponse::ReadyForQuery(ReadyForQuery { transaction_status }) => {
                match transaction_status {
                    TransactionStatus::Idle => {
                        pg.prompt_prefix = String::from("pg-async=> ");
                    }
                    TransactionStatus::InTransaction => {
                        pg.prompt_prefix = String::from("pg-async*=> ");
                    }
                    _ => todo!(),
                }
                break;
            }
        }
    }

    Ok(())
}

async fn do_query(pg: &mut Pg, backend: &mut Backend, query: SimpleQuery) -> Result<(), Box<dyn Error>> {
    backend.send_message(query).await?;

    let mut query_messages = backend.read_messages();
    while let Some(query_message) = query_messages.next() {
        eprintln!("query_message: {:?}", query_message);
        println!("{:?}", query_message.await?);
    }

    Ok(())
}
