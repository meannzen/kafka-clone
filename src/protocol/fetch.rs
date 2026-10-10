use std::io::Cursor;

use crate::{
    codec::{
        Decode, DecodeError, Encode, get_compact_array, put_compact_array, put_compact_bytes,
        put_tagged_fields, skip, skip_tagged_fields,
    },
    storage::{ClusterMetadata, log},
};

use super::Request;

#[derive(Debug)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopicRequest>,
}

#[derive(Debug)]
pub struct FetchTopicRequest {
    pub topic_id: [u8; 16],
    pub partitions: Vec<FetchPartitionRequest>,
}

#[derive(Debug)]
pub struct FetchPartitionRequest {
    pub partition: i32,
}

#[derive(Debug)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub records: Vec<u8>,
}

#[derive(Debug)]
pub struct FetchTopicResponse {
    pub topic_id: [u8; 16],
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug)]
pub struct FetchResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub topics: Vec<FetchTopicResponse>,
}

impl Decode for FetchRequest {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(Self {
            max_wait_ms: i32::decode(src)?,
            min_bytes: i32::decode(src)?,
            max_bytes: i32::decode(src)?,
            isolation_level: i8::decode(src)?,
            session_id: i32::decode(src)?,
            session_epoch: i32::decode(src)?,
            topics: get_compact_array(src)?,
        })
    }
}

impl Decode for FetchTopicRequest {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16]>::decode(src)?;
        let partitions = get_compact_array(src)?;
        skip_tagged_fields(src)?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

impl Decode for FetchPartitionRequest {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let partition = i32::decode(src)?;
        skip(src, 28)?;
        skip_tagged_fields(src)?;
        Ok(Self { partition })
    }
}

impl FetchTopicResponse {
    fn lookup(request: FetchTopicRequest, metadata: &ClusterMetadata) -> Self {
        let topic = metadata.find_topic_by_id(&request.topic_id);
        let error_code = if topic.is_some() { 0 } else { 100 };
        Self {
            topic_id: request.topic_id,
            partitions: request
                .partitions
                .into_iter()
                .map(|partition| FetchPartitionResponse {
                    partition_index: partition.partition,
                    error_code,
                    records: topic
                        .and_then(|topic| {
                            log::read_partition(&topic.topic_name, partition.partition)
                        })
                        .unwrap_or_default(),
                })
                .collect(),
        }
    }
}

impl FetchResponse {
    pub fn from_request(request: &Request, metadata: &ClusterMetadata) -> crate::Result<Self> {
        let topics = FetchRequest::decode(&mut request.payload()?)?
            .topics
            .into_iter()
            .map(|topic| FetchTopicResponse::lookup(topic, metadata))
            .collect();
        Ok(Self {
            correlation_id: request.correlation_id(),
            error_code: 0,
            throttle_time_ms: 0,
            topics,
        })
    }
}

impl Encode for FetchPartitionResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.partition_index.encode(dst);
        self.error_code.encode(dst);
        0i64.encode(dst);
        0i64.encode(dst);
        0i64.encode(dst);
        put_compact_array::<i64>(dst, &[]);
        (-1i32).encode(dst);
        put_compact_bytes(dst, &self.records);
        put_tagged_fields(dst);
    }
}

impl Encode for FetchTopicResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.topic_id.encode(dst);
        put_compact_array(dst, &self.partitions);
        put_tagged_fields(dst);
    }
}

impl Encode for FetchResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.correlation_id.encode(dst);
        put_tagged_fields(dst);
        self.throttle_time_ms.encode(dst);
        self.error_code.encode(dst);
        0i32.encode(dst);
        put_compact_array(dst, &self.topics);
        put_tagged_fields(dst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_encodes_records() {
        let resp = FetchResponse {
            correlation_id: 7,
            error_code: 0,
            throttle_time_ms: 0,
            topics: vec![FetchTopicResponse {
                topic_id: [1; 16],
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    records: vec![9, 8, 7],
                }],
            }],
        };
        let mut out = Vec::new();
        resp.encode(&mut out);
        let tail = &out[out.len() - 7..];
        assert_eq!(tail, &[4, 9, 8, 7, 0, 0, 0]);
    }
}
