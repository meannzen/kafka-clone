use std::sync::{Arc, mpsc};

use tokio::{net::TcpListener, sync::{Semaphore, broadcast}};

use crate::connection::Connection;

#[derive(Debug)]
struct Listener {
   listener: TcpListener ,
   limit_connection: Arc<Semaphore>,
   notify_shoutdown: broadcast::Sender<()>,
   shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    shutdown: Shutdown
}

#[derive(Debug)]
struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>
}
