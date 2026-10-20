use std::{
    error::Error,
    io::{self},
    net::{Shutdown, TcpListener, TcpStream},
    thread,
};

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    setup();

    let listener = TcpListener::bind("127.0.0.1:9000")?;
    info!("9000 port binding completed");

    loop {
        let (client, addr) = listener.accept()?;
        info!("client connected: {}", addr);

        thread::spawn(move || {
            if let Err(e) = handle_client(client) {
                error!("client {} error: {}", addr, e);
            }

            info!("client disconnected: {}", addr);
        });
    }
}

fn handle_client(mut client: TcpStream) -> io::Result<()> {
    let mut backend = TcpStream::connect("127.0.0.1:8080")?;
    info!("backend connected");

    // client -> backend upstream thread 생성
    let mut client_reader = client.try_clone()?;
    let mut backend_writer = backend.try_clone()?;
    let upstream = thread::spawn(move || {
        let result = io::copy(&mut client_reader, &mut backend_writer);
        let _ = backend_writer.shutdown(Shutdown::Write);
        result
    });

    // backend -> client 복사
    let downstream_result = io::copy(&mut backend, &mut client);
    let _ = client.shutdown(Shutdown::Write);

    // upstream 스레드 종료까지 대기
    let upstream_result = upstream
        .join()
        .map_err(|_| io::Error::other("upstream thread panicked"))?;

    upstream_result?;
    downstream_result?;

    Ok(())
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
