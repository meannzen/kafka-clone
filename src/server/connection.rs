use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{codec::DecodeError, protocol::Request};

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
            if let Some(request) = self.parse_request()? {
                return Ok(request);
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

    fn parse_request(&mut self) -> crate::Result<Option<Request>> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match Request::parse(&mut cursor) {
            Ok(request) => {
                let len = cursor.position() as usize;
                self.buffer.advance(len);
                Ok(Some(request))
            }
            Err(DecodeError::Incomplete) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn write_response(&mut self, body: &[u8]) -> std::io::Result<()> {
        self.stream
            .write_all(&(body.len() as i32).to_be_bytes())
            .await?;
        self.stream.write_all(body).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
