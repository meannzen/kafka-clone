use std::io::{Cursor, Read};

use bytes::Buf;

pub struct MetaParser;

#[derive(Debug)]
pub struct TopicRecord {
    //pub frame: u8,
    pub version: u8,
    pub topic_name: String,
    pub topic_id: [u8; 16],
}

#[derive(Debug)]
pub struct PartitionRecord {
    //pub frame: u8,
    pub version: u8,
    pub partition_id: i32,
    pub topic_id: [u8; 16],
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub leader: i32,
    pub leader_epoch: i32,
}

#[derive(Debug)]
pub struct FeatureLevelRecord {
    //pub frame: u8,
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
    _base_offset: u64,
    _batch_length: u32,
    _partition_leader_epoch: u32,
    _magic_byte: u8,
    _crc: i32,
    _attribute: u16,
    _last_offset_delta: u32,
    _base_timestamp: u64,
    _max_timestamp: u64,
    _producer_id: i64,
    _producer_epoch: i16,
    _base_sequence: i32,
    pub records: Vec<Record>,
}
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Not enough bytes remaining")]
    Incomplete,
    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("{0}")]
    IoError(std::io::Error),

    #[error("{0}")]
    Other(crate::Error),
}

impl From<std::io::Error> for ParseError {
    fn from(value: std::io::Error) -> Self {
        ParseError::IoError(value)
    }
}

impl TopicRecord {
    fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, ParseError> {
        let version = get_u8(src)?;
        let topic_name = get_compact_string(src)?;
        let topic_id = get_uuid(src)?;
        Ok(Self {
            version,
            topic_name,
            topic_id,
        })
    }
}
impl PartitionRecord {
    fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, ParseError> {
        let version = get_u8(src)?;
        let partition_id = get_i32(src)?;
        let topic_id = get_uuid(src)?;
        let replicas = get_compact_i32_array(src)?;
        let isr = get_compact_i32_array(src)?;
        let _removing_replicas = get_compact_i32_array(src)?;
        let _adding_replicas = get_compact_i32_array(src)?;
        let leader = get_i32(src)?;
        let leader_epoch = get_i32(src)?;
        Ok(Self {
            version,
            partition_id,
            topic_id,
            replicas,
            isr,
            leader,
            leader_epoch,
        })
    }
}
impl FeatureLevelRecord {
    fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, ParseError> {
        let version = get_u8(src)?;
        let name = get_compact_string(src)?;
        Ok(Self {
            version,
            name,
            feature_level: get_u16(src)?,
        })
    }
}

pub fn find_topic<'a>(batches: &'a [BatchRecord], name: &str) -> Option<&'a TopicRecord> {
    batches
        .iter()
        .flat_map(|batch| &batch.records)
        .find_map(|record| match record {
            Record::Topic(topic) if topic.topic_name == name => Some(topic),
            _ => None,
        })
}

pub fn find_partitions<'a>(
    batches: &'a [BatchRecord],
    topic_id: &[u8; 16],
) -> Vec<&'a PartitionRecord> {
    batches
        .iter()
        .flat_map(|batch| &batch.records)
        .filter_map(|record| match record {
            Record::Partition(partition) if &partition.topic_id == topic_id => Some(partition),
            _ => None,
        })
        .collect()
}

impl MetaParser {
    pub fn from_file(path: &str) -> Result<Vec<BatchRecord>, ParseError> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Vec<BatchRecord>, ParseError> {
        // 1. Parse BatchRecord header
        // 2. Loop or iterate through internal records
        // 3. Match record type and deserialize accordingly
        let mut batches = Vec::new();
        let mut src = Cursor::new(bytes);

        while src.position() < bytes.len() as u64 {
            let _base_offset = get_u64(&mut src)?;
            let _batch_length = get_u32(&mut src)?;
            let pos = src.position();
            let _partition_leader_epoch = get_u32(&mut src)?;

            let _magic_byte = get_u8(&mut src)?;
            let _crc = get_i32(&mut src)?;
            let _attribute: u16 = get_u16(&mut src)?;
            let _last_offset_delta = get_u32(&mut src)?;
            let _base_timestamp = get_u64(&mut src)?;
            let _max_timestamp = get_u64(&mut src)?;
            let _producer_id = get_i64(&mut src)?;
            let _producer_epoch = get_i16(&mut src)?;
            let _base_sequence = get_i32(&mut src)?;

            let records_count = get_u32(&mut src)?;
            let mut records = Vec::with_capacity(records_count as usize);
            for _ in 0..records_count {
                let length = get_varint(&mut src)?;
                let record_start = src.position();
                let _attributes = get_i8(&mut src)?;
                let _timestamp_delta = get_varint(&mut src)?;
                let _offset_delta = get_varint(&mut src)?;
                let key_length = get_varint(&mut src)?;
                if key_length > 0 {
                    skip(&mut src, key_length as usize)?;
                }

                let _value_length = get_varint(&mut src)?;
                let _frame_version = get_u8(&mut src)?;
                let type_record = get_u8(&mut src)?;

                let record = match type_record {
                    2 => Some(Record::Topic(TopicRecord::parse(&mut src)?)),
                    3 => Some(Record::Partition(PartitionRecord::parse(&mut src)?)),
                    12 => Some(Record::FeatureLevel(FeatureLevelRecord::parse(&mut src)?)),
                    _ => None,
                };
                records.extend(record);

                src.set_position(record_start + length as u64);
            }
            let batch = BatchRecord {
                _base_offset,
                _batch_length,
                _partition_leader_epoch,
                _magic_byte,
                _crc,
                _attribute,
                _last_offset_delta,
                _base_timestamp,
                _max_timestamp,
                _producer_id,
                _producer_epoch,
                _base_sequence,
                records,
            };

            batches.push(batch);
            src.set_position(pos + _batch_length as u64);
        }

        Ok(batches)
    }
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), ParseError> {
    if src.remaining() < n {
        return Err(ParseError::Incomplete);
    }
    src.advance(n);
    Ok(())
}

fn get_uvarint(src: &mut Cursor<&[u8]>) -> Result<u64, ParseError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = get_u8(src)?;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ParseError::Other("varint too long".into()))
}

fn get_varint(src: &mut Cursor<&[u8]>) -> Result<i64, ParseError> {
    let raw = get_uvarint(src)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

fn get_compact_string(src: &mut Cursor<&[u8]>) -> Result<String, ParseError> {
    let len = get_uvarint(src)?.saturating_sub(1) as usize;
    if src.remaining() < len {
        return Err(ParseError::Incomplete);
    }
    let mut buf = vec![0u8; len];
    src.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn get_compact_i32_array(src: &mut Cursor<&[u8]>) -> Result<Vec<i32>, ParseError> {
    let len = get_uvarint(src)?.saturating_sub(1) as usize;
    (0..len).map(|_| get_i32(src)).collect()
}

fn get_uuid(src: &mut Cursor<&[u8]>) -> Result<[u8; 16], ParseError> {
    if src.remaining() < 16 {
        return Err(ParseError::Incomplete);
    }
    let mut id = [0u8; 16];
    src.read_exact(&mut id)?;
    Ok(id)
}

fn get_u64(src: &mut Cursor<&[u8]>) -> Result<u64, ParseError> {
    if src.remaining() < 8 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_u64())
}

fn get_i64(src: &mut Cursor<&[u8]>) -> Result<i64, ParseError> {
    if src.remaining() < 8 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_i64())
}

fn get_u32(src: &mut Cursor<&[u8]>) -> Result<u32, ParseError> {
    if src.remaining() < 4 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_u32())
}

fn get_i32(src: &mut Cursor<&[u8]>) -> Result<i32, ParseError> {
    if src.remaining() < 4 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_i32())
}

fn get_u16(src: &mut Cursor<&[u8]>) -> Result<u16, ParseError> {
    if src.remaining() < 2 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_u16())
}

fn get_i16(src: &mut Cursor<&[u8]>) -> Result<i16, ParseError> {
    if src.remaining() < 2 {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_i16())
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, ParseError> {
    if !src.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_u8())
}
fn get_i8(src: &mut Cursor<&[u8]>) -> Result<i8, ParseError> {
    if !src.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    Ok(src.get_i8())
}
