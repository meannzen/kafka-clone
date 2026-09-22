use std::{sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
};

use crate::{
    connection::Connection,
    protocol::{ApiVersionsResponse, DescribeTopicPartitionResponse},
};
const MAX_CONNECTIONS: usize = 100;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    limit_connection: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&self) -> crate::Result<()> {
        loop {
            let permit = self.limit_connection.clone().acquire_owned().await.unwrap();
            let socket = self.accept().await?;
            let connection = Connection::new(socket);
            let mut handler = Handler {
                connection,
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
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

#[derive(Debug)]
struct Handler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    pub async fn run(&mut self) -> crate::Result<()> {
        loop {
            let request = tokio::select! {
                res = self.connection.read_request()=>res,
                _= self.shutdown.recv()=> {
                        return Ok(());
                    }
            };

            let request = match request {
                Ok(request) => request,
                Err(_) => return Ok(()),
            };

            let data = match request.header.request_api_key {
                18 => {
                    let response = ApiVersionsResponse::from_request(&request);
                    response.serialize()
                }
                75 => {
                    let response = DescribeTopicPartitionResponse::from_request(&request)?;
                    response.serialize()
                }
                key => return Err(format!("unsupported api key {}", key).into()),
            };

            self.connection.write_response(&data).await?;
        }
    }
}

pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let server = Listener {
        listener,
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

#[derive(Debug)]
pub struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}
