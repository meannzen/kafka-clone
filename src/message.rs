#![allow(dead_code)]

use core::fmt;
use std::io::Cursor;

use bytes::Buf;

#[derive(Debug)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct Message {
    pub message_size: i32,
    pub request_header: RequestHeader,
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Message {
    pub fn parse(_src: &mut Cursor<&[u8]>) -> Result<Message, Error> {
        todo!()
    }

    fn get_i16(src: &mut Cursor<&[u8]>) -> Result<i16, Error> {
        if src.remaining() < 2 {
            return Err(Error::Incomplete);
        }
        Ok(src.get_i16())
    }

    fn get_i32(src: &mut Cursor<&[u8]>) -> Result<i32, Error> {
        if src.remaining() < 4 {
            return Err(Error::Incomplete);
        }

        Ok(src.get_i32())
    }

    fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
        if src.remaining() < n {
            return Err(Error::Incomplete);
        }
        src.advance(n);
        Ok(())
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
