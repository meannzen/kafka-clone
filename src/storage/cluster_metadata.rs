use std::{io::Cursor, path::Path};

use crate::codec::{Decode, DecodeError, get_compact_array, get_compact_string};

use super::RecordBatch;

#[derive(Debug)]
pub struct TopicRecord {
    pub version: u8,
    pub topic_name: String,
    pub topic_id: [u8; 16],
}

#[derive(Debug)]
pub struct PartitionRecord {
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
    pub version: u8,
    pub name: String,
    pub feature_level: u16,
}

#[derive(Debug)]
pub enum MetadataRecord {
    Topic(TopicRecord),
    Partition(PartitionRecord),
    FeatureLevel(FeatureLevelRecord),
}

#[derive(Debug, Default)]
pub struct ClusterMetadata {
    pub records: Vec<MetadataRecord>,
}

impl ClusterMetadata {
    pub fn load(path: impl AsRef<Path>) -> crate::Result<Self> {
        let bytes = std::fs::read(path)?;
        Ok(Self::from_bytes(&bytes)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut records = Vec::new();
        for batch in RecordBatch::decode_all(bytes)? {
            for record in batch.records {
                if let Some(value) = record.value {
                    records.extend(MetadataRecord::from_value(&value)?);
                }
            }
        }
        Ok(Self { records })
    }

    pub fn find_topic(&self, name: &str) -> Option<&TopicRecord> {
        self.records.iter().find_map(|record| match record {
            MetadataRecord::Topic(topic) if topic.topic_name == name => Some(topic),
            _ => None,
        })
    }

    pub fn find_topic_by_id(&self, topic_id: &[u8; 16]) -> Option<&TopicRecord> {
        self.records.iter().find_map(|record| match record {
            MetadataRecord::Topic(topic) if &topic.topic_id == topic_id => Some(topic),
            _ => None,
        })
    }

    pub fn find_partitions(&self, topic_id: &[u8; 16]) -> Vec<&PartitionRecord> {
        self.records
            .iter()
            .filter_map(|record| match record {
                MetadataRecord::Partition(partition) if &partition.topic_id == topic_id => {
                    Some(partition)
                }
                _ => None,
            })
            .collect()
    }
}

impl MetadataRecord {
    fn from_value(value: &[u8]) -> Result<Option<Self>, DecodeError> {
        let mut src = Cursor::new(value);
        let _frame_version = u8::decode(&mut src)?;
        let record_type = u8::decode(&mut src)?;
        let record = match record_type {
            2 => Some(Self::Topic(TopicRecord::decode(&mut src)?)),
            3 => Some(Self::Partition(PartitionRecord::decode(&mut src)?)),
            12 => Some(Self::FeatureLevel(FeatureLevelRecord::decode(&mut src)?)),
            _ => None,
        };
        Ok(record)
    }
}

impl Decode for TopicRecord {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(Self {
            version: u8::decode(src)?,
            topic_name: get_compact_string(src)?,
            topic_id: <[u8; 16]>::decode(src)?,
        })
    }
}

impl Decode for PartitionRecord {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        let version = u8::decode(src)?;
        let partition_id = i32::decode(src)?;
        let topic_id = <[u8; 16]>::decode(src)?;
        let replicas = get_compact_array(src)?;
        let isr = get_compact_array(src)?;
        let _removing_replicas: Vec<i32> = get_compact_array(src)?;
        let _adding_replicas: Vec<i32> = get_compact_array(src)?;
        let leader = i32::decode(src)?;
        let leader_epoch = i32::decode(src)?;
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

impl Decode for FeatureLevelRecord {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(Self {
            version: u8::decode(src)?,
            name: get_compact_string(src)?,
            feature_level: u16::decode(src)?,
        })
    }
}
