use std::io::Cursor;

use super::{Decode, DecodeError, Encode, get_bytes, get_uvarint, put_uvarint, skip};

pub fn get_compact_len(src: &mut Cursor<&[u8]>) -> Result<usize, DecodeError> {
    Ok(get_uvarint(src)?.saturating_sub(1) as usize)
}

pub fn put_compact_len(dst: &mut Vec<u8>, len: usize) {
    put_uvarint(dst, len as u64 + 1);
}

pub fn get_compact_string(src: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let len = get_compact_len(src)?;
    Ok(String::from_utf8(get_bytes(src, len)?)?)
}

pub fn put_compact_string(dst: &mut Vec<u8>, value: &str) {
    put_compact_bytes(dst, value.as_bytes());
}

pub fn put_compact_bytes(dst: &mut Vec<u8>, value: &[u8]) {
    put_compact_len(dst, value.len());
    dst.extend_from_slice(value);
}

pub fn get_compact_array<T: Decode>(src: &mut Cursor<&[u8]>) -> Result<Vec<T>, DecodeError> {
    let len = get_compact_len(src)?;
    (0..len).map(|_| T::decode(src)).collect()
}

pub fn put_compact_array<T: Encode>(dst: &mut Vec<u8>, values: &[T]) {
    put_compact_len(dst, values.len());
    for value in values {
        value.encode(dst);
    }
}

pub fn get_nullable_string(src: &mut Cursor<&[u8]>) -> Result<Option<String>, DecodeError> {
    let len = i16::decode(src)?;
    if len < 0 {
        return Ok(None);
    }
    Ok(Some(String::from_utf8(get_bytes(src, len as usize)?)?))
}

pub fn skip_tagged_fields(src: &mut Cursor<&[u8]>) -> Result<(), DecodeError> {
    let count = get_uvarint(src)?;
    for _ in 0..count {
        let _tag = get_uvarint(src)?;
        let size = get_uvarint(src)? as usize;
        skip(src, size)?;
    }
    Ok(())
}

pub fn put_tagged_fields(dst: &mut Vec<u8>) {
    dst.push(0);
}
