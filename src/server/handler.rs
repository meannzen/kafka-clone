use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{protocol, storage::ClusterMetadata};

use super::{connection::Connection, shutdown::Shutdown};

#[derive(Debug)]
pub(super) struct Handler {
    pub(super) connection: Connection,
    pub(super) shutdown: Shutdown,
    pub(super) metadata: Arc<ClusterMetadata>,
    pub(super) _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    pub(super) async fn run(&mut self) -> crate::Result<()> {
        loop {
            let request = tokio::select! {
                res = self.connection.read_request() => res,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let request = match request {
                Ok(request) => request,
                Err(_) => return Ok(()),
            };

            let body = protocol::respond(&request, &self.metadata)?;
            self.connection.write_response(&body).await?;
        }
    }
}
