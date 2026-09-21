use codecrafters_kafka::{PORT, Result, service};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT))
        .await
        .expect("Failed to bind listener");

    service::run(listener, tokio::signal::ctrl_c()).await?;
    Ok(())
}
