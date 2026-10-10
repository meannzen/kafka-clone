mod connection;
mod handler;
mod shutdown;

use std::{sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
};

use crate::storage::ClusterMetadata;

use self::handler::Handler;
pub use self::{connection::Connection, shutdown::Shutdown};

const MAX_CONNECTIONS: usize = 100;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    metadata: Arc<ClusterMetadata>,
    limit_connection: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&self) -> crate::Result<()> {
        loop {
            let permit = self.limit_connection.clone().acquire_owned().await.unwrap();
            let socket = self.accept().await?;
            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                metadata: self.metadata.clone(),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    println!("{:?} connection failed", err);
                }
                drop(permit);
            });
        }
    }

    async fn accept(&self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => return Ok(stream),

                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }

                    eprintln!("accept failed, backing off for {}s: {:?}", backoff, err);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;

                    backoff *= 2;
                }
            }
        }
    }
}

pub async fn run(
    listener: TcpListener,
    metadata: ClusterMetadata,
    shutdown: impl Future,
) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let server = Listener {
        listener,
        metadata: Arc::new(metadata),
        limit_connection: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                println!("{:?} failed to accept", err);
            }
        }
        _ = shutdown => {
            println!("shutting down");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;
    Ok(())
}
