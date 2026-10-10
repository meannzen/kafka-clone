use std::io::Cursor;

use bytes::Buf;

use super::{Decode, DecodeError, Encode};

macro_rules! impl_int {
    ($($ty:ty => $get:ident),* $(,)?) => {$(
        impl Decode for $ty {
            fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
                ensure(src, size_of::<$ty>())?;
                Ok(src.$get())
            }
        }

        impl Encode for $ty {
            fn encode(&self, dst: &mut Vec<u8>) {
                dst.extend_from_slice(&self.to_be_bytes());
            }
        }
    )*};
}

impl_int!(
    u8 => get_u8,
    i8 => get_i8,
    u16 => get_u16,
    i16 => get_i16,
    u32 => get_u32,
    i32 => get_i32,
    u64 => get_u64,
    i64 => get_i64,
);

impl Decode for bool {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        Ok(u8::decode(src)? != 0)
    }
}

impl Encode for bool {
    fn encode(&self, dst: &mut Vec<u8>) {
        dst.push(u8::from(*self));
    }
}

impl Decode for [u8; 16] {
    fn decode(src: &mut Cursor<&[u8]>) -> Result<Self, DecodeError> {
        ensure(src, 16)?;
        let mut id = [0u8; 16];
        src.copy_to_slice(&mut id);
        Ok(id)
    }
}

impl Encode for [u8; 16] {
    fn encode(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(self);
    }
}

fn ensure(src: &Cursor<&[u8]>, n: usize) -> Result<(), DecodeError> {
    if src.remaining() < n {
        return Err(DecodeError::Incomplete);
    }
    Ok(())
}

pub fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), DecodeError> {
    ensure(src, n)?;
    src.advance(n);
    Ok(())
}

pub fn get_bytes(src: &mut Cursor<&[u8]>, n: usize) -> Result<Vec<u8>, DecodeError> {
    ensure(src, n)?;
    let bytes = src.chunk()[..n].to_vec();
    src.advance(n);
    Ok(bytes)
}

pub fn get_uvarint(src: &mut Cursor<&[u8]>) -> Result<u64, DecodeError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = u8::decode(src)?;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(DecodeError::Invalid("varint too long"))
}

pub fn get_varint(src: &mut Cursor<&[u8]>) -> Result<i64, DecodeError> {
    let raw = get_uvarint(src)?;
    Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
}

pub fn put_uvarint(dst: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        dst.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_round_trip() {
        let mut b = Vec::new();
        put_uvarint(&mut b, 1);
        put_uvarint(&mut b, 300);
        assert_eq!(b, vec![1, 0xAC, 0x02]);

        let mut src = Cursor::new(&b[..]);
        assert_eq!(get_uvarint(&mut src).unwrap(), 1);
        assert_eq!(get_uvarint(&mut src).unwrap(), 300);
    }

    #[test]
    fn varint_zigzag() {
        let bytes = [0x00, 0x01, 0x02, 0x03];
        let mut src = Cursor::new(&bytes[..]);
        let values: Vec<i64> = (0..4).map(|_| get_varint(&mut src).unwrap()).collect();
        assert_eq!(values, vec![0, -1, 1, -2]);
    }

    #[test]
    fn short_input_is_incomplete() {
        let bytes = [0u8; 3];
        let mut src = Cursor::new(&bytes[..]);
        assert!(matches!(
            i32::decode(&mut src),
            Err(DecodeError::Incomplete)
        ));
    }
}
