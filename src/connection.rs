#![allow(dead_code)]

use std::io::Cursor;

use bytes::{Buf, BytesMut};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::message::Message;

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

    pub async fn read_message(&mut self) -> crate::Result<Message> {
        loop {
            if let Ok(message) = self.parse_message() {
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

    fn parse_message(&mut self) -> crate::Result<Message> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match Message::parse_message(&mut cursor) {
            Ok(message) => {
                let len = cursor.position() as usize;
                self.buffer.advance(len);
                Ok(message)
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn write_message(&mut self, message: Message) -> std::io::Result<()> {
        self.write_i32(message.message_size).await?;
        self.write_i32(message.request_header.correlation_id)
            .await?;
        self.write_i16(message.body.error_code).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_i32(&mut self, value: i32) -> io::Result<()> {
        self.stream.write_i32(value).await?;
        Ok(())
    }

    async fn write_i16(&mut self, value: i16) -> io::Result<()> {
        self.stream.write_i16(value).await?;
        Ok(())
    }
}
