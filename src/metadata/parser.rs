pub struct MetaParser;

#[derive(Debug)]
pub struct TopicRecord {
    pub frame: u8,
    pub version: u8,
    pub topic_name: String,
    pub topic_id: [u8; 16],
}

#[derive(Debug)]
pub struct PartitionRecord {
    pub frame: u8,
    pub version: u8,
    pub partition_id: u16,
    pub topic_id: [u8; 16],
}

#[derive(Debug)]
pub struct FeatureLevelRecord {
    pub frame: u8,
    pub version: u8,
    pub name: String,
    pub feature_level: u16,
}

#[derive(Debug)]
pub enum Record {
    Topic(TopicRecord),
    Partition(PartitionRecord),
    FeatureLevel(FeatureLevelRecord),
}

#[derive(Debug)]
pub struct BatchRecord {
    _base_offset: u8,
    _batch_length: u32,
    _partition_leader_epoch: u32,
    _magic_byte: u8,
    _crc: i32,
    _attribute: u16,
    _last_offset_delta: u16,
    _base_timestamp: u64,
    _max_timestamp: u64,
    _producer_id: i64,
    _producer_epoch: i16,
    _base_bequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Not enough bytes remaining: needed {needed}, available {available}")]
    Incomplete { needed: usize, available: usize },
    #[error("Invalid magic byte: {0}")]
    InvalidMagicByte(u8),
    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

impl MetaParser {
    pub fn from_file(_path: &str) -> crate::Result<BatchRecord> {
        todo!()
    }

    pub fn from_bytes(_metadata: &[u8]) -> crate::Result<BatchRecord> {
        todo!()
        // 1. Parse BatchRecord header
        // 2. Loop or iterate through internal records
        // 3. Match record type and deserialize accordingly
    }
}
