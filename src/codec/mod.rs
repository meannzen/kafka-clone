mod compact;
mod primitives;

use std::io::Cursor;

pub use compact::{
    get_compact_array, get_compact_len, get_compact_string, get_nullable_string, put_compact_array,
    put_compact_bytes, put_compact_len, put_compact_string, put_tagged_fields, skip_tagged_fields,
};
pub use primitives::{get_bytes, get_uvarint, get_varint, put_uvarint, skip};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("not enough bytes remaining")]
    Incomplete,
    #[error("invalid utf-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("{0}")]
    Invalid(&'static str),
}

pub trait Decode: Sized {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError>;
}

pub trait Encode {
    fn encode(&self, dst: &mut Vec<u8>);
}
