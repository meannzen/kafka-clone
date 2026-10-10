use crate::protocol::{Request, produce::response::ProduceResponse};

#[derive(Debug)]
pub struct ProduceRequest {
    pub header: RequestHeader,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<TopicData>,
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[derive(Debug)]
pub struct TopicData {
    pub name: String,
    pub partitions: Vec<PartitionData>,
}

#[derive(Debug)]
pub struct PartitionData {
    pub id: i32,
    pub record_batches_size: i32,
    pub record_batch: RecordBatch,
}

#[derive(Debug)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug)]
pub struct Record {
    pub size: i32,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key_length: i32,
    pub key: Option<Vec<u8>>,
    pub value_length: i32,
    pub value: Option<Vec<u8>>,
    pub headers_length: i32,
}

impl ProduceRequest {
    pub fn from_request(_req: &Request) -> Result<ProduceResponse, crate::codec::DecodeError> {
        todo!()
    }
}
