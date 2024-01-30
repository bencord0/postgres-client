use std::{
    error::Error,
    net::{TcpListener, TcpStream},
    time::Duration,
};

use rpsql::{
    messages::frontend::{FrontendMessage, Termination},
    Backend, Frontend,
};

fn main() -> Result<(), Box<dyn Error>> {
    let pg = Pg::bind("127.0.0.1:54322")?;
    println!("Listening on 127.0.0.1:54322");

    for mut frontend in pg.connections() {
        println!("New connection from frontend");

        let mut backend = pg.connect("127.0.0.1:54321")?;
        println!("New connection to backend");

        for startup_request in frontend.read_startup_messages()? {
            backend.send_message(startup_request)?;

            for startup_response in backend.read_startup_messages()? {
                frontend.send_message(startup_response)?;
            }
        }

        for frontend_message in frontend.read_messages()? {
            backend.send_message(frontend_message.clone())?;

            if FrontendMessage::Termination(Termination) == frontend_message {
                break;
            }

            for backend_message in backend.read_messages()? {
                frontend.send_message(backend_message)?;
            }
        }

        drop(backend);
        drop(frontend);
        println!("Connection closed");
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

    fn connect(&self, target: &str) -> Result<Backend, Box<dyn Error>> {
        let stream = TcpStream::connect(target)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let backend = Backend::new(stream);
        Ok(backend)
    }

    fn connections(&self) -> impl Iterator<Item = Frontend> + '_ {
        self.listener
            .incoming()
            .filter_map(Result::ok)
            .map(Frontend::new)
    }
}
