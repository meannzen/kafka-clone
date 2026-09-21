use bytes::{ BytesMut};
use tokio::{io::BufWriter, net::TcpStream};

use crate::Message;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut
}


impl Connection {
   pub fn new(stream: TcpStream)->Self {
       Self { stream: BufWriter::new(stream), buffer: BytesMut::with_capacity(2 * 1024) }
   }

   pub async fn read_message(&self)->crate::Result<Message>{
       todo!()
   }

   pub async  fn write_message(&mut self, message: Message)->std::io::Result<()>{
       Ok(())
   }
}
