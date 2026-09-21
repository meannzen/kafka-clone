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
    pub body: Body,
}

#[derive(Debug)]
pub struct Body {
    pub error_code: i16,
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Message {
    pub fn parse_message(src: &mut Cursor<&[u8]>) -> Result<Message, Error> {
        let message_size = get_i32(src)?;
        let request_api_key = get_i16(src)?;
        let request_api_version = get_i16(src)?;
        let correlation_id = get_i32(src)?;

        let request_header = RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id,
        };

        let mut error_code: i16 = 0;
        if request_api_version > 4 {
            error_code = 35;
        }

        let body = Body { error_code };

        Ok(Message {
            message_size,
            request_header,
            body,
        })
    }
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

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
