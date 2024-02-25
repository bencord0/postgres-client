use std::{
    error::Error,
    net::{IpAddr, SocketAddr, TcpStream},
    time::Duration,
};

use rpsql::{
    messages::{
        backend::{
            BackendMessage, CommandComplete, DataRow, EmptyQueryResponse, NoticeMessage,
            RowDescription,
        },
        frontend::{SimpleQuery, Termination},
        ssl::{SSLRequest, SSLResponse},
        startup::{Startup, StartupResponse},
    },
    state::{Authentication, BackendKeyData, ParameterStatus, ReadyForQuery, TransactionStatus},
    Backend,
};

use clap::Parser;

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

    #[clap(default_value_t = true, long)]
    request_ssl: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut pg = Pg::new();

    let host: IpAddr = args.host.parse()?;
    let sockaddr: SocketAddr = (host, args.port).into();
    let mut backend = pg.connect(sockaddr)?;

    if args.request_ssl {
        let ssl_message = SSLRequest;
        backend.send_message(ssl_message)?;
        let SSLResponse::N = backend.read_ssl_message()? else {
            return Err("expected SSL answer".into());
        };
    }

    let mut startup_message = Startup::new();
    startup_message.add_parameter("user", &args.user);
    startup_message.add_parameter("database", &args.database);
    startup_message.add_parameter("application_name", "rpsql-client");
    startup_message.add_parameter("client_encoding", "UTF8");
    backend.send_message(startup_message)?;

    do_startup(&mut pg, &mut backend)?;
    let mut prompt = rustyline::DefaultEditor::new()?;

    loop {
        match prompt.readline(pg.prompt_prefix.as_str()) {
            Ok(line) => {
                let query = SimpleQuery::new(line);
                do_query(&mut pg, &mut backend, query)?;
            }
            Err(err) => {
                eprintln!("EOF: {err}");
                break;
            }
        }
    }

    let termination = Termination;
    backend.send_message(termination)?;

    Ok(())
}

fn do_startup(pg: &mut Pg, backend: &mut Backend) -> Result<(), Box<dyn Error>> {
    for backend_startup_message in backend.read_startup_messages()? {
        match backend_startup_message {
            StartupResponse::Authentication(Authentication::Ok) => {
                println!("authentication ok");
                pg.authentication = Some(Authentication::Ok);
            }

            StartupResponse::ParameterStatus(ParameterStatus { name, value }) => {
                println!("parameter status: {name}, {value}");
                pg.parameters.insert(name, value);
            }

            StartupResponse::BackendKeyData(BackendKeyData {
                process_id,
                secret_key,
            }) => {
                println!("backend data: process_id = {process_id}");
                pg.key_data = Some(BackendKeyData {
                    process_id,
                    secret_key,
                });
            }

            StartupResponse::ReadyForQuery(ReadyForQuery { transaction_status }) => {
                println!("ready for query: {transaction_status}");

                match transaction_status {
                    TransactionStatus::Idle => {
                        pg.prompt_prefix = "=>".into();
                    }
                    TransactionStatus::InTransaction => {
                        pg.prompt_prefix = "*>".into();
                    }
                    _ => todo!(),
                }
                break;
            }
        }
    }

    Ok(())
}

fn do_query(pg: &mut Pg, backend: &mut Backend, query: SimpleQuery) -> Result<(), Box<dyn Error>> {
    backend.send_message(query)?;

    for message in backend.read_messages()? {
        match message {
            BackendMessage::RowDescription(row_description) => {
                pg.row_description = Some(row_description);
            }

            BackendMessage::DataRow(DataRow { fields }) => {
                let field_names = pg.row_description.clone().unwrap_or_default().field_names();
                assert_eq!(field_names.len(), fields.len());
                println!();
                for (name, value) in field_names.into_iter().zip(fields) {
                    println!("  {} = {}", name, value.unwrap_or_else(|| "NULL".into()));
                }
            }

            BackendMessage::CommandComplete(CommandComplete { tag }) => {
                println!("command complete: {}", tag);
                let _ = pg.row_description.take();
            }

            BackendMessage::EmptyQueryResponse(EmptyQueryResponse) => {
                println!("empty query response");
                let _ = pg.row_description.take();
            }

            BackendMessage::ReadyForQuery { .. } => {
                println!("all done");
                break;
            }

            BackendMessage::NoticeMessage(NoticeMessage {
                severity,
                code,
                message,
            }) => {
                println!("notice: severity = {severity}, code = {code}, message = {message}");
            }

            _ => {
                println!("client: unhandled message: {:?}", message);
                break;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default)]
struct Pg {
    authentication: Option<Authentication>,
    parameters: std::collections::HashMap<String, String>,
    key_data: Option<BackendKeyData>,

    // Query State
    row_description: Option<RowDescription>,

    // Prompt state
    prompt_prefix: String,
}

impl Pg {
    fn new() -> Self {
        Self::default()
    }

    fn connect(&self, target: SocketAddr) -> Result<Backend, Box<dyn Error>> {
        let stream = TcpStream::connect(target)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let backend = Backend::new(stream);
        Ok(backend)
    }
}
