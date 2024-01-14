use std::io::{self, Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BytesMut};
use monoio::{
    buf::{IoBufMut, SliceMut},
    io::AsyncReadRent,
};
use smallvec::SmallVec;

use crate::{
    protocol::TInputProtocol,
    thrift::{
        CowBytes, TFieldIdentifier, TListIdentifier, TMapIdentifier, TMessageIdentifier,
        TMessageType, TSetIdentifier, TStructIdentifier, TType,
    },
    CodecError, CodecErrorKind,
};

const VERSION_1: u32 = 0x80010000;
const VERSION_MASK: u32 = 0xffff0000;

const MOST_COMMON_DEPTH: usize = 16;

#[inline]
fn field_type_from_u8(ttype: u8) -> Result<TType, CodecError> {
    let ttype: TType = ttype.try_into().map_err(|_| {
        CodecError::new(
            CodecErrorKind::InvalidData,
            format!("invalid ttype {}", ttype),
        )
    })?;

    Ok(ttype)
}

// Read more data(at least to_read).
pub async fn read_more_at_least<T: AsyncReadRent>(
    mut io: T,
    buffer: &mut BytesMut,
    to_read: usize,
) -> std::io::Result<()> {
    // We will reserve at least that much buffer to save syscalls.
    const MIN_CAPACITY: usize = 4096;
    buffer.reserve(to_read.max(MIN_CAPACITY));

    let mut read = buffer.len();
    let end = buffer.capacity();
    let at_least = read + to_read;
    while read < at_least {
        let buf = std::mem::take(buffer);
        let slice = unsafe { SliceMut::new_unchecked(buf, read, end) };
        let (r, b) = io.read(slice).await;
        *buffer = b.into_inner();
        let n = r?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        read += n;
        unsafe { buffer.set_init(read) };
    }
    Ok(())
}

pub struct TBinaryProtocol<T, A> {
    pub(crate) trans: T,
    // this buffer is only used for async decoder impl.
    pub(crate) attachment: A,
}

impl<T, A> TBinaryProtocol<T, A> {
    #[inline]
    pub fn into_inner(self) -> (T, A) {
        (self.trans, self.attachment)
    }
    #[inline]
    pub fn from_parts(trans: T, attachment: A) -> Self {
        Self { trans, attachment }
    }
}

impl<T> TBinaryProtocol<T, Cursor<BytesMut>> {
    pub fn new(io: T) -> Self {
        Self {
            trans: io,
            attachment: Cursor::new(BytesMut::new()),
        }
    }
}

impl<T: AsyncReadRent> TBinaryProtocol<T, Cursor<BytesMut>> {
    async fn fill_at_least(&mut self, n: usize) -> std::io::Result<()> {
        let rem = self.attachment.remaining();
        if rem >= n {
            return Ok(());
        }
        let to_read = n - rem;
        read_more_at_least(&mut self.trans, self.attachment.get_mut(), to_read).await
    }
}

impl<'x, A: 'static> TInputProtocol<'x> for TBinaryProtocol<Cursor<&'x [u8]>, A> {
    type Buf<'b> = Cursor<&'b [u8]>
    where
        Self: 'b;

    fn read_message_begin(&mut self) -> Result<TMessageIdentifier, CodecError> {
        let size: i32 = self.trans.read_i32::<BigEndian>()?;

        if size > 0 {
            return Err(CodecError::new(
                CodecErrorKind::BadVersion,
                "Missing version in ReadMessageBegin".to_string(),
            ));
        }
        let type_u8 = (size & 0xf) as u8;

        let message_type = TMessageType::try_from(type_u8).map_err(|_| {
            CodecError::new(
                CodecErrorKind::InvalidData,
                format!("invalid message type {}", type_u8),
            )
        })?;

        let version = size & (VERSION_MASK as i32);
        if version != (VERSION_1 as i32) {
            return Err(CodecError::new(
                CodecErrorKind::BadVersion,
                "Bad version in ReadMessageBegin",
            ));
        }

        let name = CowBytes::Borrowed(self.read_string()?);

        let sequence_number = self.read_i32()?;
        Ok(TMessageIdentifier::new(name, message_type, sequence_number))
    }

    #[inline]
    fn read_message_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_struct_begin(&mut self) -> Result<TStructIdentifier, CodecError> {
        Ok(TStructIdentifier::new(None))
    }

    #[inline]
    fn read_struct_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_field_begin(&mut self) -> Result<TFieldIdentifier, CodecError> {
        let field_type_byte = self.read_byte()?;
        let field_type: TType = field_type_byte.try_into().map_err(|_| {
            CodecError::new(
                CodecErrorKind::InvalidData,
                format!("invalid ttype {}", field_type_byte),
            )
        })?;
        let id = match field_type {
            TType::Stop => Ok(0),
            _ => self.read_i16(),
        }?;
        Ok(TFieldIdentifier::new(None, field_type, Some(id)))
    }

    #[inline]
    fn read_field_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_list_begin(&mut self) -> Result<TListIdentifier, CodecError> {
        let element_type = self.read_byte().and_then(field_type_from_u8)?;
        let size = self.read_i32()?;
        Ok(TListIdentifier::new(element_type, size as usize))
    }

    #[inline]
    fn read_list_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_set_begin(&mut self) -> Result<TSetIdentifier, CodecError> {
        let element_type = self.read_byte().and_then(field_type_from_u8)?;
        let size = self.read_i32()?;
        Ok(TSetIdentifier::new(element_type, size as usize))
    }

    #[inline]
    fn read_set_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_map_begin(&mut self) -> Result<TMapIdentifier, CodecError> {
        let key_type = self.read_byte().and_then(field_type_from_u8)?;
        let value_type = self.read_byte().and_then(field_type_from_u8)?;
        let size = self.read_i32()?;
        Ok(TMapIdentifier::new(key_type, value_type, size as usize))
    }

    #[inline]
    fn read_map_end(&mut self) -> Result<(), CodecError> {
        Ok(())
    }

    #[inline]
    fn read_byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.trans.read_u8()?)
    }

    #[inline]
    fn read_bool(&mut self) -> Result<bool, CodecError> {
        Ok(self.read_i8()? != 0)
    }

    #[inline]
    fn read_i8(&mut self) -> Result<i8, CodecError> {
        self.trans.read_i8().map_err(Into::into)
    }

    #[inline]
    fn read_i16(&mut self) -> Result<i16, CodecError> {
        self.trans.read_i16::<BigEndian>().map_err(Into::into)
    }

    #[inline]
    fn read_i32(&mut self) -> Result<i32, CodecError> {
        self.trans.read_i32::<BigEndian>().map_err(Into::into)
    }

    #[inline]
    fn read_i64(&mut self) -> Result<i64, CodecError> {
        self.trans.read_i64::<BigEndian>().map_err(Into::into)
    }

    #[inline]
    fn read_double(&mut self) -> Result<f64, CodecError> {
        self.trans.read_f64::<BigEndian>().map_err(Into::into)
    }

    #[inline]
    fn read_uuid(&mut self) -> Result<[u8; 16], CodecError> {
        let mut u = [0; 16];
        self.trans.read_exact(&mut u)?;
        Ok(u)
    }

    #[inline]
    fn read_bytes(&mut self) -> Result<&'x [u8], CodecError> {
        let len = self.trans.read_i32::<BigEndian>()? as usize;
        let total = self.trans.get_ref().len();
        let pos = self.trans.position() as usize;
        let target_pos = pos + len;
        if target_pos > total {
            return Err(CodecError::new(
                CodecErrorKind::InvalidData,
                format!("invalid bytes length {len}"),
            ));
        }
        self.trans.set_position(target_pos as u64);

        let ptr = self.trans.get_ref().as_ptr();
        Ok(unsafe { std::slice::from_raw_parts(ptr.add(pos), len) })
    }

    #[inline]
    fn read_string(&mut self) -> Result<&'x str, CodecError> {
        let data = self.read_bytes()?;
        if data.is_empty() {
            return Ok("");
        }
        if let Some(chunk) = std::str::Utf8Chunks::new(data).next() {
            let s = chunk.valid();
            if s.len() == data.len() {
                return Ok(s);
            }
        }
        Err(CodecError::new(
            CodecErrorKind::InvalidData,
            "not a valid utf8 string",
        ))
    }

    fn skip_field(&mut self, ttype: TType) -> Result<(), CodecError> {
        const BINARY_BASIC_TYPE_FIXED_SIZE: [usize; 17] = [
            0,  // TType::Stop
            0,  // TType::Void
            1,  // TType::Bool
            1,  // TType::I8
            8,  // TType::Double
            0,  // NAN
            2,  // TType::I16
            0,  // NAN
            4,  // TType::I32
            0,  // NAN
            8,  // TType::I64
            0,  // TType::Binary
            0,  // TType::Struct
            0,  // TType::Map
            0,  // TType::List
            0,  // TType::Set
            16, // TType::Uuid
        ];

        macro_rules! pop {
            ($stack:expr) => {
                match $stack.pop() {
                    Some(last) => last,
                    None => break,
                }
            };
        }
        macro_rules! read_ttype {
            ($trans: expr) => {{
                let field_type_byte = $trans.get_u8();
                let field_type: TType = field_type_byte.try_into().map_err(|_| {
                    CodecError::new(
                        CodecErrorKind::InvalidData,
                        format!("invalid ttype {field_type_byte}"),
                    )
                })?;
                field_type
            }};
        }

        macro_rules! require_data {
            ($self: expr, $n: expr) => {
                if $self.trans.remaining() < $n {
                    return Err(CodecError::invalid_data());
                }
            };
        }

        let mut stack = SkipDataStack::new();
        let mut current = SkipData::Other(ttype);

        loop {
            match current {
                SkipData::Other(TType::Struct) => {
                    require_data!(self, 1);
                    let field_type = read_ttype!(self.trans);

                    // fast skip(only for better performance)
                    let size =
                        unsafe { *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(field_type as usize) };
                    if size != 0 {
                        require_data!(self, 2 + size);
                        self.trans.advance(2 + size);
                        continue;
                    }

                    match field_type {
                        TType::Stop => {
                            current = pop!(stack);
                        }
                        _ => {
                            require_data!(self, 2);
                            self.trans.advance(2); // field id
                            stack.push(current);
                            current = SkipData::Other(field_type);
                        }
                    }
                }
                SkipData::Other(ttype) => match ttype {
                    TType::Bool | TType::I8 => {
                        require_data!(self, 1);
                        self.trans.advance(1);
                        current = pop!(stack);
                    }
                    TType::Double | TType::I64 => {
                        require_data!(self, 8);
                        self.trans.advance(8);
                        current = pop!(stack);
                    }
                    TType::I16 => {
                        require_data!(self, 2);
                        self.trans.advance(2);
                        current = pop!(stack);
                    }
                    TType::I32 => {
                        require_data!(self, 4);
                        self.trans.advance(4);
                        current = pop!(stack);
                    }
                    TType::Binary => {
                        require_data!(self, 4);
                        let len = self.trans.get_i32() as usize;
                        require_data!(self, len);
                        self.trans.advance(len);
                        current = pop!(stack);
                    }
                    TType::Uuid => {
                        require_data!(self, 16);
                        self.trans.advance(16);
                        current = pop!(stack);
                    }
                    TType::List | TType::Set => {
                        require_data!(self, 5);
                        let element_type = read_ttype!(self.trans);
                        let element_len = self.trans.get_i32() as u32;
                        let size = unsafe {
                            *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type as usize)
                        };
                        if size != 0 {
                            let skip = element_len as usize * size;
                            require_data!(self, skip);
                            self.trans.advance(skip);
                            current = pop!(stack);
                        } else {
                            current =
                                SkipData::Collection(element_len, [element_type, element_type]);
                        }
                    }
                    TType::Map => {
                        require_data!(self, 6);
                        let element_type = read_ttype!(self.trans);
                        let element_type2 = read_ttype!(self.trans);
                        let element_len = self.trans.get_i32() as u32;
                        let size = unsafe {
                            *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type as usize)
                        };
                        let size2 = unsafe {
                            *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type2 as usize)
                        };
                        if size != 0 && size2 != 0 {
                            let skip = element_len as usize * (size + size2);
                            require_data!(self, skip);
                            self.trans.advance(skip);
                            current = pop!(stack);
                        } else {
                            current = SkipData::Collection(
                                element_len * 2,
                                [element_type, element_type2],
                            );
                        }
                    }
                    _ => {
                        return Err(CodecError::new(
                            CodecErrorKind::InvalidData,
                            format!("invalid ttype {}, normal type is expected", ttype as u8),
                        ));
                    }
                },
                SkipData::Collection(len, ttypes) => {
                    if len == 0 {
                        current = pop!(stack);
                        continue;
                    }
                    current = SkipData::Other(ttypes[(len & 1) as usize]);
                    stack.push(SkipData::Collection(len - 1, ttypes));
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn buf<'a>(&'a mut self) -> &'a mut Self::Buf<'x>
    where
        'x: 'a,
    {
        &mut self.trans
    }
}

#[derive(Debug)]
enum SkipData {
    Collection(u32, [TType; 2]),
    Other(TType),
}
type SkipDataStack = SmallVec<[SkipData; MOST_COMMON_DEPTH]>;
