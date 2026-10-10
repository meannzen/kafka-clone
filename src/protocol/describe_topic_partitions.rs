use std::io::Cursor;

use crate::{
    codec::{
        Decode, DecodeError, Encode, get_compact_array, get_compact_string, put_compact_array,
        put_compact_string, put_tagged_fields, skip_tagged_fields,
    },
    storage::ClusterMetadata,
};

use super::Request;

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequest {
    pub topic_names: Vec<String>,
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

struct TopicName(String);

impl Decode for TopicName {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let name = get_compact_string(src)?;
        skip_tagged_fields(src)?;
        Ok(Self(name))
    }
}

impl Decode for DescribeTopicPartitionsRequest {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let topic_names: Vec<TopicName> = get_compact_array(src)?;
        if topic_names.is_empty() {
            return Err(DecodeError::Invalid("no topics requested"));
        }
        Ok(Self {
            topic_names: topic_names.into_iter().map(|name| name.0).collect(),
        })
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

    pub fn lookup(topic_name: String, metadata: &ClusterMetadata) -> Self {
        let Some(topic) = metadata.find_topic(&topic_name) else {
            return Self::unknown(topic_name);
        };

        let mut partitions: Vec<PartitionResponse> = metadata
            .find_partitions(&topic.topic_id)
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
    pub fn from_request(request: &Request, metadata: &ClusterMetadata) -> crate::Result<Self> {
        let mut topic_names =
            DescribeTopicPartitionsRequest::decode(&mut request.payload()?)?.topic_names;
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
}

impl Encode for PartitionResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.error_code.encode(dst);
        self.partition_index.encode(dst);
        self.leader_id.encode(dst);
        self.leader_epoch.encode(dst);
        put_compact_array(dst, &self.replica_nodes);
        put_compact_array(dst, &self.isr_nodes);
        put_compact_array::<i32>(dst, &[]); // eligible_leader_replicas
        put_compact_array::<i32>(dst, &[]); // last_known_elr
        put_compact_array::<i32>(dst, &[]); // offline_replicas
        put_tagged_fields(dst);
    }
}

impl Encode for TopicResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.error_code.encode(dst);
        put_compact_string(dst, &self.topic_name);
        self.topic_id.encode(dst);
        self.is_internal.encode(dst);
        put_compact_array(dst, &self.partitions);
        self.topic_authorized_operations.encode(dst);
        put_tagged_fields(dst);
    }
}

impl Encode for DescribeTopicPartitionResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.correlation_id.encode(dst);
        put_tagged_fields(dst);
        self.throttle_time_ms.encode(dst);
        put_compact_array(dst, &self.topics);
        dst.push(0xFF); // next_cursor = null
        put_tagged_fields(dst);
    }
}
