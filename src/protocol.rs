use bytes::Buf;
use core::fmt;
use std::io::Cursor;

#[derive(Debug)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub header: RequestHeader,
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Request {
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        let message_size = get_i32(src)?;
        let request_api_key = get_i16(src)?;
        let request_api_version = get_i16(src)?;
        let correlation_id = get_i32(src)?;

        if message_size < 8 {
            return Err(Error::Other("invalid message_size".into()));
        }
        let remaining = message_size as usize - 8;
        if src.remaining() < remaining {
            return Err(Error::Incomplete);
        }
        src.advance(remaining);

        Ok(Request {
            message_size,
            header: RequestHeader {
                request_api_key,
                request_api_version,
                correlation_id,
            },
        })
    }
}

impl ApiVersionsResponse {
    pub fn from_request(req: &Request) -> Self {
        let error_code = if req.header.request_api_version > 4 {
            35
        } else {
            0
        };

        ApiVersionsResponse {
            correlation_id: req.header.correlation_id,
            error_code,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            }],
            throttle_time_ms: 0,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();

        body.extend_from_slice(&self.correlation_id.to_be_bytes());

        body.extend_from_slice(&self.error_code.to_be_bytes());

        body.push((self.api_keys.len() as u8) + 1);

        for api in &self.api_keys {
            body.extend_from_slice(&api.api_key.to_be_bytes());
            body.extend_from_slice(&api.min_version.to_be_bytes());
            body.extend_from_slice(&api.max_version.to_be_bytes());
            body.push(0);
        }

        body.extend_from_slice(&self.throttle_time_ms.to_be_bytes());

        body.push(0);

        let mut msg = Vec::with_capacity(4 + body.len());
        msg.extend_from_slice(&(body.len() as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
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

impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => write!(f, "stream ended early"),
            Error::Other(err) => write!(f, "{err}"),
        }
    }
}
