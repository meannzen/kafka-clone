use codecrafters_kafka::{
    PORT, Result, server,
    storage::{ClusterMetadata, log},
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let metadata = ClusterMetadata::load(log::segment_path("__cluster_metadata", 0))
        .unwrap_or_else(|err| {
            eprintln!("failed to load cluster metadata log: {err}");
            ClusterMetadata::default()
        });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT))
        .await
        .expect("Failed to bind listener");

    server::run(listener, metadata, tokio::signal::ctrl_c()).await?;
    Ok(())
}
