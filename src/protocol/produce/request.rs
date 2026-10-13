use crate::codec::{
    Decode, get_bytes, get_compact_array, get_compact_len, get_compact_nullable_string,
    get_compact_string, skip_tagged_fields,
};

#[derive(Debug)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<Topic>,
}

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct Partition {
    pub index: i32,
    pub records: Vec<u8>,
}

impl Decode for ProduceRequest {
    fn decode(src: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::codec::DecodeError> {
        let transactional_id = get_compact_nullable_string(src)?;
        let acks = i16::decode(src)?;
        let timeout_ms = i32::decode(src)?;
        let topics = get_compact_array(src)?;
        skip_tagged_fields(src)?;
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topics,
        })
    }
}

impl Decode for Topic {
    fn decode(src: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::codec::DecodeError> {
        let name = get_compact_string(src)?;
        let partitions = get_compact_array(src)?;
        skip_tagged_fields(src)?;
        Ok(Self { name, partitions })
    }
}

impl Decode for Partition {
    fn decode(src: &mut std::io::Cursor<&[u8]>) -> Result<Self, crate::codec::DecodeError> {
        let index = i32::decode(src)?;
        let len = get_compact_len(src)?;
        let records = get_bytes(src, len)?;
        skip_tagged_fields(src)?;
        Ok(Self { index, records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_produce_request() {
        let mut bytes = vec![0x00];
        bytes.extend((-1i16).to_be_bytes());
        bytes.extend(1500i32.to_be_bytes());
        bytes.push(2);
        bytes.push(4);
        bytes.extend(b"foo");
        bytes.push(2);
        bytes.extend(0i32.to_be_bytes());
        bytes.push(4);
        bytes.extend([1, 2, 3]);
        bytes.extend([0, 0, 0]);

        let req = ProduceRequest::decode(&mut std::io::Cursor::new(&bytes[..])).unwrap();
        assert_eq!(req.transactional_id, None);
        assert_eq!(req.acks, -1);
        assert_eq!(req.timeout_ms, 1500);
        assert_eq!(req.topics[0].name, "foo");
        assert_eq!(req.topics[0].partitions[0].index, 0);
        assert_eq!(req.topics[0].partitions[0].records, vec![1, 2, 3]);
    }
}
