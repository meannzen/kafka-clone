use bytes::Buf;
use core::fmt;
use std::io::Cursor;

use crate::metadata::parser::{self, BatchRecord};

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
pub struct PartitionResponse {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug)]
pub struct TopicResponse {
    pub error_code: i16,
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<PartitionResponse>,
    pub topic_authorized_operations: i32,
}

#[derive(Debug)]
pub struct DescribeTopicPartitionResponse {
    pub correlation_id: i32,
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicResponse>,
}

#[derive(Debug)]
pub struct FetchResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicResponse>,
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
        let body = src.chunk()[..remaining].to_vec();
        src.advance(body.len());

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
                ApiVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                },
                ApiVersion {
                    api_key: 75,
                    min_version: 0,
                    max_version: 0,
                },
                ApiVersion {
                    api_key: 1,
                    min_version: 0,
                    max_version: 16,
                },
            ],
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

impl TopicResponse {
    pub fn unknown(topic_name: String) -> Self {
        Self {
            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
            topic_name,
            topic_id: [0u8; 16],
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0,
        }
    }

    pub fn lookup(topic_name: String, metadata: &[BatchRecord]) -> Self {
        let Some(topic) = parser::find_topic(metadata, &topic_name) else {
            return Self::unknown(topic_name);
        };

        let mut partitions: Vec<PartitionResponse> =
            parser::find_partitions(metadata, &topic.topic_id)
                .into_iter()
                .map(|partition| PartitionResponse {
                    error_code: 0,
                    partition_index: partition.partition_id,
                    leader_id: partition.leader,
                    leader_epoch: partition.leader_epoch,
                    replica_nodes: partition.replicas.clone(),
                    isr_nodes: partition.isr.clone(),
                })
                .collect();
        partitions.sort_by_key(|partition| partition.partition_index);

        Self {
            error_code: 0,
            topic_name,
            topic_id: topic.topic_id,
            is_internal: false,
            partitions,
            topic_authorized_operations: 0x0000_0df8,
        }
    }
}

impl DescribeTopicPartitionResponse {
    pub fn from_request(request: &Request, metadata: &[BatchRecord]) -> crate::Result<Self> {
        let mut topic_names = parse_topic_names(&request.body)?;
        topic_names.sort();

        Ok(Self {
            correlation_id: request.correlation_id(),
            throttle_time_ms: 0,
            topics: topic_names
                .into_iter()
                .map(|name| TopicResponse::lookup(name, metadata))
                .collect(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();

        body.extend_from_slice(&self.correlation_id.to_be_bytes());
        body.push(0); // TAG_BUFFER

        body.extend_from_slice(&self.throttle_time_ms.to_be_bytes());

        body.push((self.topics.len() as u8) + 1);

        for topic in &self.topics {
            body.extend_from_slice(&topic.error_code.to_be_bytes());

            let name_bytes = topic.topic_name.as_bytes();
            body.push((name_bytes.len() as u8) + 1);
            body.extend_from_slice(name_bytes);

            body.extend_from_slice(&topic.topic_id);

            body.push(u8::from(topic.is_internal));

            body.push((topic.partitions.len() as u8) + 1);
            for partition in &topic.partitions {
                body.extend_from_slice(&partition.error_code.to_be_bytes());
                body.extend_from_slice(&partition.partition_index.to_be_bytes());
                body.extend_from_slice(&partition.leader_id.to_be_bytes());
                body.extend_from_slice(&partition.leader_epoch.to_be_bytes());
                put_compact_i32_array(&mut body, &partition.replica_nodes);
                put_compact_i32_array(&mut body, &partition.isr_nodes);
                put_compact_i32_array(&mut body, &[]); // eligible_leader_replicas
                put_compact_i32_array(&mut body, &[]); // last_known_elr
                put_compact_i32_array(&mut body, &[]); // offline_replicas
                body.push(0); // TAG_BUFFER
            }

            body.extend_from_slice(&topic.topic_authorized_operations.to_be_bytes());

            // TAG_BUFFER for this topic
            body.push(0);
        }

        // next_cursor = null
        body.push(0xFF);

        // final TAG_BUFFER this wtf
        body.push(0);

        // ── Prepend message size ────────────────────────────
        let mut msg = Vec::with_capacity(4 + body.len());
        msg.extend_from_slice(&(body.len() as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }
}

fn put_compact_i32_array(body: &mut Vec<u8>, values: &[i32]) {
    body.push((values.len() as u8) + 1);
    for value in values {
        body.extend_from_slice(&value.to_be_bytes());
    }
}

impl FetchResponse {
    pub fn from_request(request: &Request) -> Self {
        Self {
            correlation_id: request.correlation_id(),
            error_code: 0,
            throttle_time_ms: 0,
            topics: vec![],
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();

        body.extend_from_slice(&self.correlation_id.to_be_bytes());  // 4
        body.push(0); // TAG_BUFFER
        body.extend_from_slice(&self.error_code.to_be_bytes()); //2

        body.extend_from_slice(&self.throttle_time_ms.to_be_bytes()); //2
        let session_id : i32 = 0;
        body.extend_from_slice(&session_id.to_be_bytes());
        //  topic length
        body.push((self.topics.len() as u8) + 1);
        for topic in &self.topics {
            body.extend_from_slice(&topic.error_code.to_be_bytes());

            let name_bytes = topic.topic_name.as_bytes();
            body.push((name_bytes.len() as u8) + 1);
            body.extend_from_slice(name_bytes);

            body.extend_from_slice(&topic.topic_id);

            body.push(u8::from(topic.is_internal));

            body.push((topic.partitions.len() as u8) + 1);
            for partition in &topic.partitions {
                body.extend_from_slice(&partition.error_code.to_be_bytes());
                body.extend_from_slice(&partition.partition_index.to_be_bytes());
                body.extend_from_slice(&partition.leader_id.to_be_bytes());
                body.extend_from_slice(&partition.leader_epoch.to_be_bytes());
                put_compact_i32_array(&mut body, &partition.replica_nodes);
                put_compact_i32_array(&mut body, &partition.isr_nodes);
                put_compact_i32_array(&mut body, &[]); // eligible_leader_replicas
                put_compact_i32_array(&mut body, &[]); // last_known_elr
                put_compact_i32_array(&mut body, &[]); // offline_replicas
                body.push(0); // TAG_BUFFER
            }

            body.extend_from_slice(&topic.topic_authorized_operations.to_be_bytes());

            // TAG_BUFFER for this topic
            body.push(0);
        }

            // TAG_BUFFER for this topic fuck up here
            body.push(0);


        // ── Prepend message size ────────────────────────────
        let mut msg = Vec::with_capacity(4 + body.len());
        msg.extend_from_slice(&(body.len() as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }
}

fn parse_topic_names(body: &[u8]) -> crate::Result<Vec<String>> {
    let mut cursor = std::io::Cursor::new(body);

    let client_id_len = get_i16(&mut cursor)?;
    if client_id_len > 0 {
        let client_id_len = client_id_len as usize;
        if cursor.remaining() < client_id_len {
            return Err(Error::Incomplete.into());
        }
        cursor.advance(client_id_len);
    }

    let _tag = get_u8(&mut cursor)?;

    let topics_len = get_u8(&mut cursor)? as usize;
    if topics_len == 0 {
        return Err("empty topics array".into());
    }
    let num_topics = topics_len - 1;
    if num_topics == 0 {
        return Err("no topics requested".into());
    }

    let mut topic_names = Vec::with_capacity(num_topics);
    for _ in 0..num_topics {
        let name_len = get_u8(&mut cursor)? as usize;
        if name_len == 0 {
            return Err("null topic name".into());
        }
        let name_len = name_len - 1;

        if cursor.remaining() < name_len {
            return Err(Error::Incomplete.into());
        }

        let name_bytes = &cursor.chunk()[..name_len];
        topic_names.push(String::from_utf8_lossy(name_bytes).into_owned());
        cursor.advance(name_len);

        let _tag = get_u8(&mut cursor)?; // per-topic TAG_BUFFER
    }

    Ok(topic_names)
}

fn get_u8(src: &mut std::io::Cursor<&[u8]>) -> Result<u8, Error> {
    if src.remaining() < 1 {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
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
