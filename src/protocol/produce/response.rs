use crate::{
    codec::{Decode, Encode, put_compact_array, put_compact_string, put_tagged_fields},
    protocol::{Request, produce::request::ProduceRequest},
    storage::ClusterMetadata,
};

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

#[derive(Debug)]
pub struct ProducePartitionResponse {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

#[derive(Debug)]
pub struct ProduceReponseTopic {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug)]
pub struct ProduceResponse {
    pub correlation_id: i32,
    pub topics: Vec<ProduceReponseTopic>,
    pub throttle_time_ms: i32,
}

impl ProduceResponse {
    pub fn from_request(req: &Request, metadata: &ClusterMetadata) -> crate::Result<Self> {
        let produce_request = ProduceRequest::decode(&mut req.payload()?)?;
        let topics = produce_request
            .topics
            .iter()
            .map(|topic| {
                let partition_ids: Vec<i32> = metadata
                    .find_topic(&topic.name)
                    .map(|t| {
                        metadata
                            .find_partitions(&t.topic_id)
                            .iter()
                            .map(|p| p.partition_id)
                            .collect()
                    })
                    .unwrap_or_default();

                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        if partition_ids.contains(&partition.index) {
                            ProducePartitionResponse {
                                index: partition.index,
                                error_code: 0,
                                base_offset: 0,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            }
                        } else {
                            ProducePartitionResponse {
                                index: partition.index,
                                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: -1,
                            }
                        }
                    })
                    .collect();

                ProduceReponseTopic {
                    name: topic.name.clone(),
                    partitions,
                }
            })
            .collect();

        Ok(Self {
            correlation_id: req.correlation_id(),
            topics,
            throttle_time_ms: 0,
        })
    }
}

impl Encode for ProducePartitionResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.index.encode(dst);
        self.error_code.encode(dst);
        self.base_offset.encode(dst);
        self.log_append_time_ms.encode(dst);
        self.log_start_offset.encode(dst);
        put_compact_array::<i32>(dst, &[]);
        dst.push(0);
        put_tagged_fields(dst);
    }
}

impl Encode for ProduceReponseTopic {
    fn encode(&self, dst: &mut Vec<u8>) {
        put_compact_string(dst, &self.name);
        put_compact_array(dst, &self.partitions);
        put_tagged_fields(dst);
    }
}

impl Encode for ProduceResponse {
    fn encode(&self, dst: &mut Vec<u8>) {
        self.correlation_id.encode(dst);
        put_tagged_fields(dst);
        put_compact_array(dst, &self.topics);
        self.throttle_time_ms.encode(dst);
        put_tagged_fields(dst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_encodes_response() {
        let response = ProduceResponse {
            correlation_id: 7,
            topics: vec![ProduceReponseTopic {
                name: "foo".into(),
                partitions: vec![ProducePartitionResponse {
                    index: 0,
                    error_code: 3,
                    base_offset: -1,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                }],
            }],
            throttle_time_ms: 0,
        };
        let mut dst = Vec::new();
        response.encode(&mut dst);

        let mut expected = 7i32.to_be_bytes().to_vec();
        expected.push(0);
        expected.push(2);
        expected.push(4);
        expected.extend(b"foo");
        expected.push(2);
        expected.extend(0i32.to_be_bytes());
        expected.extend(3i16.to_be_bytes());
        expected.extend((-1i64).to_be_bytes());
        expected.extend((-1i64).to_be_bytes());
        expected.extend((-1i64).to_be_bytes());
        expected.extend([1, 0, 0]);
        expected.push(0);
        expected.extend(0i32.to_be_bytes());
        expected.push(0);
        assert_eq!(dst, expected);
    }
}
