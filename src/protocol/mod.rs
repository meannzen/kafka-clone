pub mod api_versions;
pub mod describe_topic_partitions;
pub mod fetch;
pub mod produce;

use std::io::Cursor;

use crate::{
    codec::{Decode, DecodeError, Encode, get_bytes, get_nullable_string, skip_tagged_fields},
    protocol::produce::response::ProduceResponse,
    storage::ClusterMetadata,
};

use self::{
    api_versions::ApiVersionsResponse, describe_topic_partitions::DescribeTopicPartitionResponse,
    fetch::FetchResponse,
};

#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl TryFrom<i16> for ApiKey {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Produce),
            1 => Ok(Self::Fetch),
            18 => Ok(Self::ApiVersions),
            75 => Ok(Self::DescribeTopicPartitions),
            other => Err(other),
        }
    }
}

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
    pub body: Vec<u8>,
}

impl Request {
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let message_size = i32::decode(src)?;
        let request_api_key = i16::decode(src)?;
        let request_api_version = i16::decode(src)?;
        let correlation_id = i32::decode(src)?;

        if message_size < 8 {
            return Err(DecodeError::Invalid("invalid message_size"));
        }
        let body = get_bytes(src, message_size as usize - 8)?;

        Ok(Request {
            message_size,
            header: RequestHeader {
                request_api_key,
                request_api_version,
                correlation_id,
            },
            body,
        })
    }

    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    pub fn payload(&self) -> Result<Cursor<&[u8]>, DecodeError> {
        let mut src = Cursor::new(&self.body[..]);
        let _client_id = get_nullable_string(&mut src)?;
        skip_tagged_fields(&mut src)?;
        Ok(src)
    }
}

pub fn respond(request: &Request, metadata: &ClusterMetadata) -> crate::Result<Vec<u8>> {
    let api_key = request.header.request_api_key;
    let mut body = Vec::new();

    match ApiKey::try_from(api_key) {
        Ok(ApiKey::ApiVersions) => ApiVersionsResponse::from_request(request).encode(&mut body),
        Ok(ApiKey::Fetch) => FetchResponse::from_request(request, metadata)?.encode(&mut body),
        Ok(ApiKey::DescribeTopicPartitions) => {
            DescribeTopicPartitionResponse::from_request(request, metadata)?.encode(&mut body)
        }
        Ok(ApiKey::Produce) => ProduceResponse::from_request(request, metadata)?.encode(&mut body),
        Err(_) => {
            return Err(format!("unsupported api key {}", api_key).into());
        }
    }

    Ok(body)
}
