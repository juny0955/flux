use flux::proxy::Proxy;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
    Proxy::new("127.0.0.1:19000")?.start()?;
    Ok(())
}
