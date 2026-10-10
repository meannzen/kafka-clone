use std::io::Cursor;

use bytes::Buf;

use crate::codec::{Decode, DecodeError, get_bytes, get_varint};

#[derive(Debug)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

impl RecordBatch {
    pub fn decode_all(bytes: &[u8]) -> Result<Vec<Self>, DecodeError> {
        let mut src = Cursor::new(bytes);
        let mut batches = Vec::new();
        while src.has_remaining() {
            batches.push(Self::decode(&mut src)?);
        }
        Ok(batches)
    }
}

impl Decode for RecordBatch {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let base_offset = i64::decode(src)?;
        let batch_length = i32::decode(src)?;
        let end = src.position() + batch_length as u64;

        let partition_leader_epoch = i32::decode(src)?;
        let magic = i8::decode(src)?;
        let crc = u32::decode(src)?;
        let attributes = i16::decode(src)?;
        let last_offset_delta = i32::decode(src)?;
        let base_timestamp = i64::decode(src)?;
        let max_timestamp = i64::decode(src)?;
        let producer_id = i64::decode(src)?;
        let producer_epoch = i16::decode(src)?;
        let base_sequence = i32::decode(src)?;

        let records_count = i32::decode(src)?;
        let records = (0..records_count)
            .map(|_| Record::decode(src))
            .collect::<Result<_, _>>()?;

        src.set_position(end);

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl Decode for Record {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let length = get_varint(src)?;
        let start = src.position();

        let attributes = i8::decode(src)?;
        let timestamp_delta = get_varint(src)?;
        let offset_delta = get_varint(src)?;
        let key = get_varint_bytes(src)?;
        let value = get_varint_bytes(src)?;

        src.set_position(start + length as u64);

        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
        })
    }
}

fn get_varint_bytes(src: &mut Cursor<&[u8]>) -> Result<Option<Vec<u8>>, DecodeError> {
    let len = get_varint(src)?;
    if len < 0 {
        return Ok(None);
    }
    Ok(Some(get_bytes(src, len as usize)?))
}
