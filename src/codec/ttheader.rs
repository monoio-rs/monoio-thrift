//! TTheader is a transport protocol designed by CloudWeGo.
//!
//! For more information, please visit https://www.cloudwego.io/docs/kitex/reference/transport_protocol_ttheader/

use std::{io, ptr::copy_nonoverlapping};
use std::collections::HashMap;

use smallvec::SmallVec;
use smol_str::SmolStr;

use monoio_codec::{Decoded, Decoder, Encoder};

use bytes::{Buf, BufMut};
use num_enum::TryFromPrimitive;

pub type HeaderMap = HashMap<SmolStr, SmolStr>;

#[derive(Clone)]
pub struct TTHeader {
    pub header_length: u32,
    pub payload_length: u32,
    pub seq_id: i32,
    pub flags: u16,
    pub protocol_id: ProtocolId,
    // int key < IntMetaKey::INDEX_TABLE_SIZE
    pub int_headers: [Option<SmolStr>; IntMetaKey::INDEX_TABLE_SIZE],
    // int key >= IntMetaKey::INDEX_TABLE_SIZE
    pub int_headers_ext: SmallVec<[(u16, SmolStr); 2]>,
    pub str_headers: HeaderMap,
    pub acl_token: Option<SmolStr>,
}

impl Default for TTHeader {
    fn default() -> Self {
        Self {
            header_length: 0,
            payload_length: 0,
            seq_id: 0,
            flags: 0,
            protocol_id: ProtocolId::Binary,
            int_headers: Default::default(),
            int_headers_ext: Default::default(),
            str_headers: Default::default(),
            acl_token: None,
        }
    }
}

impl TTHeader {
    #[inline]
    pub fn new_for_encode(payload_length_hint: u32) -> Self {
        Self {
            header_length: 0,
            payload_length: payload_length_hint,
            seq_id: 0,
            flags: 0,
            protocol_id: ProtocolId::Binary,
            int_headers: Default::default(),
            int_headers_ext: Default::default(),
            str_headers: Default::default(),
            acl_token: None,
        }
    }

    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    // TODO: now only supports io::Error
    fn decode_header(&mut self, total_length: u32, src: &mut bytes::BytesMut) -> io::Result<()> {
        #[inline]
        unsafe fn read_u8_unchecked(buf: &[u8], index: &mut usize) -> u8 {
            let val = *buf.get_unchecked(*index);
            *index += 1;
            val
        }

        #[inline]
        unsafe fn read_u16_unchecked(buf: &[u8], index: &mut usize) -> u16 {
            let val = u16::from_be_bytes(
                buf.get_unchecked(*index..*index + 2)
                    .try_into()
                    .unwrap_unchecked(),
            );
            *index += 2;
            val
        }

        macro_rules! read_u16_checked {
            ($buf: ident, $index: ident, $len: expr) => {{
                if $index + 2 > $len as usize {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid data"));
                }
                unsafe { read_u16_unchecked($buf, &mut $index) }
            }};
        }

        #[inline]
        unsafe fn read_raw_str_unchecked(buf: &[u8], len: usize, index: &mut usize) -> SmolStr {
            let val = {
                let str = SmolStr::new(std::str::from_utf8_unchecked(
                    buf.get_unchecked(*index..(*index + len)),
                ));
                str
            };
            *index += len;
            val
        }

        macro_rules! read_str_checked {
            ($buf: ident, $index: ident, $len: expr) => {{
                let val_len = read_u16_checked!($buf, $index, $len);
                if $index + val_len as usize > $len as usize
                    || val_len as usize > MAX_HEADER_STRING_LENGTH
                {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid data"));
                }
                unsafe { read_raw_str_unchecked($buf, val_len as usize, &mut $index) }
            }};
        }

        src.advance(2); // skip magic
        self.flags = src.get_u16();
        self.seq_id = src.get_i32();
        let header_size = src.get_u16();
        self.header_length = header_size as u32 * 4;
        if self.header_length as usize > src.len() || header_size < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid header length",
            ));
        }
        let header_buf = src.split_to(self.header_length as usize);
        self.payload_length = total_length - self.header_length - 10;
        let buf = header_buf.as_ref();
        let mut index = 0;
        // It's safe when checked header_size >= 1
        if let Ok(protocol_id) = unsafe { ProtocolId::try_from(read_u8_unchecked(buf, &mut index)) }
        {
            self.protocol_id = protocol_id;
        }
        index += 1; // TODO: support transform

        let mut _padding_num = 0usize;

        while index < self.header_length as usize {
            // It's safe because while expr
            let info_id = unsafe { read_u8_unchecked(buf, &mut index) };
            match info_id {
                info::INFO_PADDING => {
                    _padding_num += 1;
                    continue;
                }
                info::INFO_KEY_VALUE => {
                    let kv_size = read_u16_checked!(buf, index, self.header_length);
                    if kv_size as usize > MAX_NUM_HEADERS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("too many headers: {kv_size}, max: {MAX_NUM_HEADERS}"),
                        ));
                    }
                    // TODO: reserve
                    for _ in 0..kv_size {
                        let key = read_str_checked!(buf, index, self.header_length);
                        let val = read_str_checked!(buf, index, self.header_length);
                        self.str_headers.insert(key, val);
                    }
                }
                info::INFO_INT_KEY_VALUE => {
                    let kv_size = read_u16_checked!(buf, index, self.header_length);
                    for _ in 0..kv_size {
                        let key = read_u16_checked!(buf, index, self.header_length);
                        let val = read_str_checked!(buf, index, self.header_length);

                        if (key as usize) < IntMetaKey::INDEX_TABLE_SIZE {
                            // It's safe because `if expr`
                            unsafe {
                                *self.int_headers.get_unchecked_mut(key as usize) = Some(val);
                            }
                        } else {
                            self.int_headers_ext.push((key, val));
                        }
                    }
                }
                info::ACL_TOKEN_KEY_VALUE => {
                    self.acl_token = Some(read_str_checked!(buf, index, self.header_length));
                }
                _ => {
                    // We are not able to decode the protocol anymore, since we don't know the
                    // layout
                    let msg = format!("unexpected info id in ttheader: {info_id}");
                    tracing::error!("{}", msg);
                    return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct TTHeaderDecoder;

impl TTHeaderDecoder {
    pub const fn new() -> Self {
        Self
    }
}

impl Decoder for TTHeaderDecoder {
    type Item = TTHeader;
    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        if src.len() < MIN_HEADER_LENGTH {
            return Ok(Decoded::InsufficientAtLeast(MIN_HEADER_LENGTH));
        }

        if src[4..HEADER_DETECT_LENGTH] == [0x10, 0x00] {
            let mut header_length = [0; 2];
            unsafe { copy_nonoverlapping(src.as_ptr().add(12), header_length.as_mut_ptr(), 2) };
            let header_length = u16::from_be_bytes(header_length) as usize * 4;
            if src.len() < header_length + MIN_HEADER_LENGTH {
                return Ok(Decoded::InsufficientAtLeast(
                    header_length + MIN_HEADER_LENGTH,
                ));
            }

            let mut length = [0; 4];
            unsafe { copy_nonoverlapping(src.as_ptr(), length.as_mut_ptr(), 4) };
            let length = u32::from_be_bytes(length);

            src.advance(4);

            // decode ttheader
            let mut ttheader = TTHeader::new();
            ttheader.decode_header(length, src)?; // TODO: which error type?
            Ok(Decoded::Some(ttheader))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "illegal ttheader"))
        }
    }
}

#[derive(Default)]
pub struct TTHeaderEncoder;

impl TTHeaderEncoder {
    pub const fn new() -> Self {
        Self
    }
}

impl Encoder<TTHeader> for TTHeaderEncoder {
    type Error = io::Error;

    fn encode(&mut self, item: TTHeader, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        #[inline]
        fn put_str(s: &SmolStr, dst: &mut bytes::BytesMut) {
            dst.put_u16(s.len() as u16);
            dst.put_slice(s.as_bytes());
        }

        dst.reserve(4 * 1024); // cap 4k
        let zero_index = dst.len();
        unsafe {
            dst.advance_mut(4);
        }

        // tt header magic
        dst.put_u16(TT_HEADER_MAGIC);
        // flags
        dst.put_u16(item.flags);
        dst.put_i32(item.seq_id);

        // Alloc 2-byte space as header length
        unsafe {
            dst.advance_mut(2);
        }

        dst.put_u8(item.protocol_id as u8);
        dst.put_u8(0); // TODO: transform_ids_num

        // Write string KV start.
        dst.put_u8(info::INFO_KEY_VALUE);
        dst.put_u16(item.str_headers.len() as u16);

        for (key, val) in item.str_headers.iter() {
            put_str(key, dst);
            put_str(val, dst);
        }

        // Write int KV start.
        dst.put_u8(info::INFO_INT_KEY_VALUE);
        let int_kv_index = dst.len();
        let mut int_kv_len = 0_u16;
        unsafe {
            dst.advance_mut(2);
        }

        for (key, val) in item.int_headers.iter().enumerate() {
            if let Some(val) = val {
                dst.put_u16(key as u16);
                put_str(val, dst);
                int_kv_len += 1;
            }
        }

        for (key, val) in item.int_headers_ext.iter() {
            dst.put_u16(*key);
            dst.put_slice(val.as_bytes());
            int_kv_len += 1;
        }

        // fill int kv length
        let mut buf = &mut dst[int_kv_index..int_kv_index + 2];
        buf.put_u16(int_kv_len);

        // write padding
        let overflow = (dst.len() - 14 - zero_index) % 4;
        let padding = (4 - overflow) % 4;
        (0..padding).for_each(|_| dst.put_u8(0));

        // fill header length
        let header_size = dst.len() - zero_index;
        let mut buf = &mut dst[zero_index + 12..zero_index + 12 + 2];
        buf.put_u16(((header_size - 14) / 4).try_into().unwrap());
        tracing::trace!(
            "encode ttheader write header size: {}",
            (header_size - 14) / 4
        );

        // fill length
        let size = dst.len() + item.payload_length as usize - zero_index;
        let mut buf = &mut dst[zero_index..zero_index + 4];
        buf.put_u32((size - 4) as u32);
        tracing::trace!("encode ttheader write length size: {}", size - 4);
        Ok(())
    }
}

pub struct TTHeaderPayload<T> {
    pub ttheader: TTHeader,
    pub payload: Option<T>,
}

impl<T> Clone for TTHeaderPayload<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            ttheader: self.ttheader.clone(),
            payload: self.payload.clone(),
        }
    }
}

impl<T> Default for TTHeaderPayload<T> {
    fn default() -> Self {
        Self {
            ttheader: Default::default(),
            payload: Default::default(),
        }
    }
}

impl<T> TTHeaderPayload<T> {
    fn new() -> Self {
        Self {
            ttheader: TTHeader::new(),
            payload: None,
        }
    }
}

pub struct TTHeaderPayloadDecoder<T> {
    payload_decoder: T,
}

impl<T> TTHeaderPayloadDecoder<T> {
    pub fn new(payload_decoder: T) -> Self {
        Self { payload_decoder }
    }
}

impl<T: Decoder> Decoder for TTHeaderPayloadDecoder<T>
where
    T::Error: From<io::Error>,
{
    type Item = TTHeaderPayload<T::Item>;
    type Error = T::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        if src.len() < MIN_HEADER_LENGTH {
            return Ok(Decoded::InsufficientAtLeast(MIN_HEADER_LENGTH));
        }

        if src[4..HEADER_DETECT_LENGTH] == [0x10, 0x00] {
            let mut length = [0; 4];
            unsafe { copy_nonoverlapping(src.as_ptr(), length.as_mut_ptr(), 4) };
            let length = u32::from_be_bytes(length);
            if src.len() < length as usize + 4 {
                return Ok(Decoded::InsufficientAtLeast(length as usize + 4));
            }
            src.advance(4);

            let mut item = Self::Item::new();
            item.ttheader.decode_header(length, src)?;
            match self.payload_decoder.decode(src) {
                Ok(Decoded::Some(payload)) => item.payload = Some(payload),
                Err(e) => return Err(e),
                // we have already checked sufficient size, so it's err if Insufficient
                _ => return Err(io::Error::new(io::ErrorKind::Other, "illegal payload").into()),
            };
            Ok(Decoded::Some(item))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "illegal ttheader").into())
        }
    }
}

pub struct TTHeaderPayloadEncoder<T> {
    payload_encoder: T,
}

impl<T> TTHeaderPayloadEncoder<T> {
    pub fn new(payload_encoder: T) -> Self {
        Self { payload_encoder }
    }
}

impl<T, E: Encoder<T>> Encoder<TTHeaderPayload<T>> for TTHeaderPayloadEncoder<E> {
    type Error = E::Error;

    fn encode(
        &mut self,
        item: TTHeaderPayload<T>,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let zero_index = dst.len();
        let mut ttheader_encoder = TTHeaderEncoder {};
        ttheader_encoder.encode(item.ttheader, dst)?;
        self.payload_encoder
            .encode(item.payload.expect("payload must some"), dst)?;
        // fill length
        let size = dst.len() - zero_index;
        let mut buf = &mut dst[zero_index..zero_index + 4];
        buf.put_u32((size - 4) as u32);
        tracing::trace!("encode ttheader write length size: {}", size - 4);
        Ok(())
    }
}

#[derive(Default)]
pub struct RawPayloadCodec;

impl RawPayloadCodec {
    pub const fn new() -> Self {
        Self
    }
}

impl Decoder for RawPayloadCodec {
    type Item = bytes::Bytes;

    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Decoded<Self::Item>, Self::Error> {
        Ok(Decoded::Some(bytes::Bytes::from(src.split())))
    }
}

impl Encoder<bytes::Bytes> for RawPayloadCodec {
    type Error = io::Error;

    fn encode(&mut self, item: bytes::Bytes, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len());
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// 4-bytes length + 2-bytes magic
/// https://www.cloudwego.io/docs/kitex/reference/transport_protocol_ttheader/
const HEADER_DETECT_LENGTH: usize = 6;
const MIN_HEADER_LENGTH: usize = 14;

pub const TT_HEADER_MAGIC: u16 = 0x1000;
pub const MAX_HEADER_STRING_LENGTH: usize = 4 * 1024; // 4k
pub const MAX_NUM_HEADERS: usize = 1024; // 1k

mod info {
    pub const INFO_PADDING: u8 = 0x00;
    pub const INFO_KEY_VALUE: u8 = 0x01;
    pub const INFO_INT_KEY_VALUE: u8 = 0x10;
    pub const ACL_TOKEN_KEY_VALUE: u8 = 0x11;
}

#[derive(TryFromPrimitive, Clone, Copy, Default)]
#[repr(u8)]
pub enum ProtocolId {
    #[default]
    Binary = 0,
    Compact = 2,   // Apache Thrift compact protocol
    CompactV2 = 3, // fbthrift compact protocol
    Protobuf = 4,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, TryFromPrimitive, Debug)]
#[repr(u16)]
pub enum IntMetaKey {
    TransportType = 1,
    // framed / unframed
    LogId = 2,
    FromService = 3,
    FromCluster = 4,
    FromIdc = 5,
    ToService = 6,
    ToMethod = 9,
    ToCluster = 7,
    ToIdc = 8,
    Env = 10,
    DestAddress = 11,
    RPCTimeoutMs = 12,
    RingHashKey = 14,
    WithHeader = 16,
    ConnTimeoutMs = 17,
    TraceSpanCtx = 18,
    ShortConnection = 19,
    FromMethod = 20,
    StressTag = 21,
    MsgType = 22,
    ConnectionRecycle = 23,
    RawRingHashKey = 24,
    LBType = 25,
    ClusterShardId = 26,
}

impl IntMetaKey {
    const INDEX_TABLE_SIZE: usize = Self::ClusterShardId as usize + 1;
}
