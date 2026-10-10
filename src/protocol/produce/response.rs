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
    pub partitions: Vec<i32>,
}

#[derive(Debug)]
pub struct ProduceResponse {
    pub correlation_id: i32,
    pub topics: Vec<ProduceReponseTopic>,
    pub throttle_time_ms: i32,
}
