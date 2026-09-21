use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::protocol::{self, Request};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(2 * 1024),
        }
    }

    pub async fn read_request(&mut self) -> crate::Result<Request> {
        loop {
            if let Ok(message) = self.parse_request() {
                return Ok(message);
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Err("empty message".into());
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_request(&mut self) -> crate::Result<Request> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match protocol::Request::parse(&mut cursor) {
            Ok(reponse) => {
                let len = cursor.position() as usize;
                self.buffer.advance(len);
                Ok(reponse)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn write_response(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
