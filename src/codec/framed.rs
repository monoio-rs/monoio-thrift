use std::{io, ptr::copy_nonoverlapping};

use bytes::{Buf, BufMut};
use monoio_codec::{Decoded, Decoder, Encoder};

pub struct FramedHeader<T> {
    inner: T,
}

impl<T> FramedHeader<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: Decoder> Decoder for FramedHeader<T>
where
    T::Error: From<io::Error>,
{
    type Item = T::Item;
    type Error = T::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(Decoded::InsufficientAtLeast(4));
        }
        let length = {
            let mut length = [0; 4];
            unsafe { copy_nonoverlapping(src.as_ptr(), length.as_mut_ptr(), 4) };
            let length = i32::from_be_bytes(length);
            if length <= 0 {
                return Err(
                    io::Error::new(io::ErrorKind::Other, "illegal thrift body size").into(),
                );
            }
            length as usize
        };
        if src.len() < length + 4 {
            return Ok(Decoded::InsufficientAtLeast(length + 4));
        }

        src.advance(4);
        let mut body = src.split_to(length);
        self.inner.decode(&mut body)
    }
}

impl<T: Encoder<Item>, Item> Encoder<Item> for FramedHeader<T> {
    type Error = T::Error;

    fn encode(&mut self, item: Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let offset = dst.len();
        dst.put_i32(0);

        self.inner.encode(item, dst)?;

        let len = (dst.len() - offset - 4) as i32;
        let len_data = len.to_be_bytes();
        unsafe {
            len_data
                .as_ptr()
                .copy_to_nonoverlapping(dst.as_mut_ptr().add(offset), 4)
        };
        Ok(())
    }
}
