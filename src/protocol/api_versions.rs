use crate::codec::{Encode, put_compact_array, put_tagged_fields};

use super::{ApiKey, Request};

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

impl ApiVersion {
    fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key: api_key as i16,
            min_version,
            max_version,
        }
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
            api_keys: vec![
                ApiVersion::new(ApiKey::Produce, 0, 11),
                ApiVersion::new(ApiKey::ApiVersions, 0, 4),
                ApiVersion::new(ApiKey::DescribeTopicPartitions, 0, 0),
                ApiVersion::new(ApiKey::Fetch, 0, 16),
            ],
            throttle_time_ms: 0,
        }
    }
}

impl Encode for ApiVersion {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.api_key.encode(dst);
        self.min_version.encode(dst);
        self.max_version.encode(dst);
        put_tagged_fields(dst);
    }
}

impl Encode for ApiVersionsResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.correlation_id.encode(dst);
        self.error_code.encode(dst);
        put_compact_array(dst, &self.api_keys);
        self.throttle_time_ms.encode(dst);
        put_tagged_fields(dst);
    }
}
