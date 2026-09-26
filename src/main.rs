use codecrafters_kafka::{
    PORT, Result,
    metadata::{PATH, parser::MetaParser},
    service,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let batch_records = MetaParser::from_file(PATH).unwrap_or_else(|err| {
        eprintln!("failed to load cluster metadata log: {err}");
        Vec::new()
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT))
        .await
        .expect("Failed to bind listener");

    service::run(listener, batch_records, tokio::signal::ctrl_c()).await?;
    Ok(())
}
