use std::{
    io::{self, Cursor, Read},
    ptr::copy_nonoverlapping,
};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use monoio::{
    buf::{IoBufMut, SliceMut},
    io::AsyncReadRent,
};
use smallvec::SmallVec;

use crate::{
    protocol::{TAsyncInputProtocol, TAsyncSkipProtocol, TInputProtocol, TOutputProtocol},
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

#[inline(always)]
fn advance(cursor: &mut Cursor<BytesMut>, cnt: usize) {
    let pos = cursor.position() + cnt as u64;
    cursor.set_position(pos);
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

#[derive(Debug)]
enum SkipData {
    Collection(u32, [TType; 2]),
    Other(TType),
}
type SkipDataStack = SmallVec<[SkipData; MOST_COMMON_DEPTH]>;
pub type TBinarySkipper<IO> = TBinaryProtocol<IO, Cursor<BytesMut>>;
type PositionStack = SmallVec<[usize; MOST_COMMON_DEPTH]>;
pub type TBinaryReader<'a> = TBinaryProtocol<Cursor<&'a [u8]>, PositionStack>;
pub type TBinaryWriter<'a> = TBinaryProtocol<&'a mut BytesMut, PositionStack>;

pub struct TBinaryProtocol<T, A> {
    pub(crate) trans: T,
    // this buffer is only used for async decoder impl.
    pub(crate) attachment: A,
}

impl<T> TBinaryProtocol<T, Cursor<BytesMut>> {
    pub fn new(io: T) -> Self {
        Self {
            trans: io,
            attachment: Cursor::new(BytesMut::new()),
        }
    }
}

impl<'a> TBinaryProtocol<Cursor<&'a [u8]>, PositionStack> {
    pub fn new(trans: Cursor<&'a [u8]>) -> Self {
        Self {
            trans,
            attachment: SmallVec::new(),
        }
    }
}

impl<'a> TBinaryProtocol<&'a mut BytesMut, PositionStack> {
    pub fn new(trans: &'a mut BytesMut) -> Self {
        Self {
            trans,
            attachment: SmallVec::new(),
        }
    }

    #[inline]
    fn write_length(&mut self, len: usize) {
        let pos = self.attachment.pop().expect("illegal thrift pair");
        let len = len as i32;
        // Note: use big endian for length as thrift encoding
        self.trans[pos..pos + 4].copy_from_slice(&len.to_be_bytes());
    }
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

impl<T: AsyncReadRent> TBinaryProtocol<T, BytesMut> {
    async fn fill_at_least(&mut self, n: usize) -> std::io::Result<()> {
        let rem = self.attachment.remaining();
        if rem >= n {
            return Ok(());
        }
        let to_read = n - rem;
        read_more_at_least(&mut self.trans, &mut self.attachment, to_read).await
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
        if let Some(chunk) = data.utf8_chunks().next() {
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

macro_rules! impl_async_fn {
    (async fn $fname:ident(&mut $self:ident $(,$arg:ident: $arg_type:ty)*) -> Result<$futname:ident($out: ty)> { instant($imp:expr) }) => {
        #[inline] async fn $fname(&mut $self $(,$arg : $arg_type)*) -> Result<$out, CodecError> { $imp }
    };
    (async fn $fname:ident(&mut $self:ident $(,$arg:ident: $arg_type:ty)*) -> Result<$futname:ident($out: ty)> { $($imp:tt)* }) => {
        #[inline] async fn $fname(&mut $self $(,$arg : $arg_type)*) -> Result<$out, CodecError> { $($imp)*  }
    };
    ($(async fn $fname:ident(&mut $self:ident $(,$arg:ident: $arg_type:ty)*) -> Result<$futname:ident($out: ty)> { $($imp:tt)* })*) => {
        $(impl_async_fn!(async fn $fname(&mut $self $(,$arg : $arg_type)*) -> Result<$futname($out)> { $($imp)* });)*
    }
}

macro_rules! require_data {
    ($self: expr, $n: expr) => {
        if $self.attachment.remaining() < $n {
            $self.fill_at_least($n).await?;
        }
    };
}

impl<T: AsyncReadRent> TAsyncSkipProtocol for TBinaryProtocol<T, Cursor<BytesMut>> {
    impl_async_fn! {
        async fn skip_message(&mut self) -> Result<SkipMessage(())> {
            require_data!(self, 4);
            let size = self.attachment.get_i32();

            if size > 0 {
                return Err(CodecError::new(
                    CodecErrorKind::BadVersion,
                    "Missing version in ReadMessageBegin".to_string(),
                ));
            }

            let version = size & (VERSION_MASK as i32);
            if version != (VERSION_1 as i32) {
                return Err(CodecError::new(
                    CodecErrorKind::BadVersion,
                    "Bad version in ReadMessageBegin",
                ));
            }
            // skip name and sequence number
            require_data!(self, 4);
            let len = self.attachment.get_i32() as usize;
            require_data!(self, len + 4);
            advance(&mut self.attachment, len + 4);
            // skip struct
            self.skip_field(TType::Struct).await?;
            Ok(())
        }
        async fn skip_field(&mut self, ttype: TType) -> Result<SkipField(())> {
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
                ($attachment: expr) => {
                    {
                        let field_type_byte = $attachment.get_u8();
                        let field_type: TType = field_type_byte.try_into().map_err(|_| {
                        CodecError::new(
                            CodecErrorKind::InvalidData,
                            format!("invalid ttype {field_type_byte}"),
                        )
                        })?;
                        field_type
                    }
                };
            }

            let mut stack = SkipDataStack::new();
            let mut current = SkipData::Other(ttype);

            loop {
                match current {
                    SkipData::Other(TType::Struct) => {
                        require_data!(self, 1);
                        let field_type = read_ttype!(self.attachment);

                        // fast skip(only for better performance)
                        let size = unsafe{*BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(field_type as usize)};
                        if size != 0 {
                            require_data!(self, 2 + size);
                            advance(&mut self.attachment, 2 + size);
                            continue;
                        }

                        match field_type {
                            TType::Stop => {
                                current = pop!(stack);
                            }
                            _ => {
                                require_data!(self, 2);
                                advance(&mut self.attachment, 2); // field id
                                stack.push(current);
                                current = SkipData::Other(field_type);
                            }
                        }
                    }
                    SkipData::Other(ttype) => {
                        match ttype {
                            TType::Bool | TType::I8 => {
                                require_data!(self, 1);
                                advance(&mut self.attachment, 1);
                                current = pop!(stack);
                            },
                            TType::Double | TType::I64 => {
                                require_data!(self, 8);
                                advance(&mut self.attachment, 8);
                                current = pop!(stack);
                            },
                            TType::I16 => {
                                require_data!(self, 2);
                                advance(&mut self.attachment, 2);
                                current = pop!(stack);
                            },
                            TType::I32 => {
                                require_data!(self, 4);
                                advance(&mut self.attachment, 4);
                                current = pop!(stack);
                            },
                            TType::Binary => {
                                require_data!(self, 4);
                                let len = self.attachment.get_i32() as usize;
                                require_data!(self, len);
                                advance(&mut self.attachment, len);
                                current = pop!(stack);
                            },
                            TType::Uuid => {
                                require_data!(self, 16);
                                advance(&mut self.attachment, 16);
                                current = pop!(stack);
                            },
                            TType::List | TType::Set => {
                                require_data!(self, 5);
                                let element_type = read_ttype!(self.attachment);
                                let element_len = self.attachment.get_i32() as u32;
                                let size = unsafe{ *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type as usize) };
                                if size != 0 {
                                    let skip = element_len as usize * size;
                                    require_data!(self, skip);
                                    advance(&mut self.attachment, skip);
                                    current = pop!(stack);
                                } else {
                                    current = SkipData::Collection(element_len, [element_type, element_type]);
                                }
                            },
                            TType::Map => {
                                require_data!(self, 6);
                                let element_type = read_ttype!(self.attachment);
                                let element_type2 = read_ttype!(self.attachment);
                                let element_len = self.attachment.get_i32() as u32;
                                let size = unsafe{ *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type as usize) };
                                let size2 = unsafe{ *BINARY_BASIC_TYPE_FIXED_SIZE.get_unchecked(element_type2 as usize) };
                                if size != 0 && size2 != 0 {
                                    let skip = element_len as usize * (size + size2);
                                    require_data!(self, skip);
                                    advance(&mut self.attachment, skip);
                                    current = pop!(stack);
                                } else {
                                    current = SkipData::Collection(element_len * 2, [element_type, element_type2]);
                                }
                            }
                            _ => {
                                return Err(CodecError::new(
                                    CodecErrorKind::InvalidData,
                                    format!("invalid ttype {}, normal type is expected", ttype as u8),
                                ));
                            }
                        }
                    }
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
    }
}

impl<T: AsyncReadRent> TAsyncInputProtocol for TBinaryProtocol<T, BytesMut> {
    impl_async_fn! {
        async fn read_message_begin(&mut self) -> Result<ReadMessageBegin(TMessageIdentifier<'static>)> {
            let size = self.read_i32().await?;

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

            let name = CowBytes::Owned(self.read_string().await?);

            let sequence_number = self.read_i32().await?;
            Ok(TMessageIdentifier::new(name, message_type, sequence_number))
        }
        async fn read_message_end(&mut self) -> Result<ReadMessageEnd(())> {
            instant(Ok(()))
        }
        async fn read_struct_begin(&mut self) -> Result<ReadStructBegin(TStructIdentifier)> {
            instant(Ok(TStructIdentifier::new(None)))
        }
        async fn read_struct_end(&mut self) -> Result<ReadStructEnd(())> {
            instant(Ok(()))
        }
        async fn read_field_begin(&mut self) -> Result<ReadFieldBegin(TFieldIdentifier)> {
            let field_type_byte = self.read_byte().await?;
            let field_type = field_type_byte.try_into().map_err(|_| {
                CodecError::new(
                    CodecErrorKind::InvalidData,
                    format!("invalid ttype {}", field_type_byte),
                )
            })?;
            let id = match field_type {
                TType::Stop => Ok(0),
                _ => self.read_i16().await,
            }?;
            Ok(TFieldIdentifier::new(None, field_type, Some(id)))
        }
        async fn read_field_end(&mut self) -> Result<ReadFieldEnd(())> {
            instant(Ok(()))
        }
        async fn read_list_begin(&mut self) -> Result<ReadListBegin(TListIdentifier)> {
            let element_type = self.read_byte().await.and_then(field_type_from_u8)?;
            let size = self.read_i32().await?;
            Ok(TListIdentifier::new(element_type, size as usize))
        }
        async fn read_list_end(&mut self) -> Result<ReadListEnd(())> {
            instant(Ok(()))
        }
        async fn read_set_begin(&mut self) -> Result<ReadSetBegin(TSetIdentifier)> {
            let element_type = self.read_byte().await.and_then(field_type_from_u8)?;
            let size = self.read_i32().await?;
            Ok(TSetIdentifier::new(element_type, size as usize))
        }
        async fn read_set_end(&mut self) -> Result<ReadSetEnd(())> {
            instant(Ok(()))
        }
        async fn read_map_begin(&mut self) -> Result<ReadMapBegin(TMapIdentifier)> {
            let key_type = self.read_byte().await.and_then(field_type_from_u8)?;
            let value_type = self.read_byte().await.and_then(field_type_from_u8)?;
            let size = self.read_i32().await?;
            Ok(TMapIdentifier::new(key_type, value_type, size as usize))
        }
        async fn read_map_end(&mut self) -> Result<ReadMapEnd(())> {
            instant(Ok(()))
        }
        async fn read_byte(&mut self) -> Result<ReadByte(u8)> {
            require_data!(self, 1);
            Ok(self.attachment.get_u8())
        }
        async fn read_bool(&mut self) -> Result<ReadBool(bool)> {
            Ok(self.read_i8().await? != 0)
        }
        async fn read_i8(&mut self) -> Result<ReadI8(i8)> {
            require_data!(self, 1);
            Ok(self.attachment.get_i8())
        }
        async fn read_i16(&mut self) -> Result<ReadI16(i16)> {
            require_data!(self, 2);
            Ok(self.attachment.get_i16())
        }
        async fn read_i32(&mut self) -> Result<ReadI32(i32)> {
            require_data!(self, 4);
            Ok(self.attachment.get_i32())
        }
        async fn read_i64(&mut self) -> Result<ReadI64(i64)> {
            require_data!(self, 8);
            Ok(self.attachment.get_i64())
        }
        async fn read_double(&mut self) -> Result<ReadDouble(f64)> {
            require_data!(self, 8);
            Ok(self.attachment.get_f64())
        }
        async fn read_uuid(&mut self) -> Result<ReadUuid([u8; 16])> {
            require_data!(self, 16);
            let mut out = [0; 16];
            unsafe { copy_nonoverlapping(self.attachment.as_ptr(), out.as_mut_ptr(), 16) };
            self.attachment.advance(16);
            Ok(out)
        }
        async fn read_bytes(&mut self) -> Result<ReadBytes(Bytes)> {
            let length = self.read_i32().await? as usize;
            require_data!(self, length);
            let out = self.attachment.split_to(length).freeze();
            Ok(out)
        }
        async fn read_string(&mut self) -> Result<ReadString(Bytes)> {
            let data = self.read_bytes().await?;
            if data.is_empty() {
                return Ok(data);
            }
            if let Some(chunk) = data.utf8_chunks().next() {
                let s = chunk.valid();
                if s.len() == data.len() {
                    return Ok(data);
                }
            }
            Err(CodecError::new(
                CodecErrorKind::InvalidData,
                "not a valid utf8 string",
            ))
        }
    }
}

impl TOutputProtocol for TBinaryProtocol<&mut BytesMut, PositionStack> {
    type Buf = BytesMut;

    #[inline]
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) {
        let msg_type_u8: u8 = identifier.message_type.into();
        let version = (VERSION_1 | msg_type_u8 as u32) as i32;
        self.write_i32(version);
        self.write_bytes(identifier.name.as_bytes());
        self.write_i32(identifier.sequence_number);
    }

    #[inline(always)]
    fn write_message_end(&mut self) {}

    #[inline]
    fn write_struct_begin(&mut self, _identifier: &TStructIdentifier) {}

    #[inline(always)]
    fn write_struct_end(&mut self) {}

    #[inline]
    fn write_field_begin(&mut self, field_type: TType, id: i16) {
        let mut data: [u8; 3] = [0; 3];
        data[0] = field_type as u8;
        let id = id.to_be_bytes();
        data[1] = id[0];
        data[2] = id[1];
        self.trans.put_slice(&data);
    }

    #[inline(always)]
    fn write_field_end(&mut self) {}

    #[inline]
    fn write_field_stop(&mut self) {
        self.write_byte(TType::Stop as u8);
    }

    #[inline]
    fn write_list_begin(&mut self, identifier: &TListIdentifier) {
        self.write_byte(identifier.element_type.into());
        self.attachment.push(self.trans.len());
        self.write_i32(identifier.size as i32);
    }

    #[inline]
    fn write_list_end(&mut self, len: usize) {
        self.write_length(len);
    }

    #[inline]
    fn write_set_begin(&mut self, identifier: &TSetIdentifier) {
        self.write_byte(identifier.element_type.into());
        self.attachment.push(self.trans.len());
        self.write_i32(identifier.size as i32);
    }

    #[inline]
    fn write_set_end(&mut self, len: usize) {
        self.write_length(len);
    }

    #[inline]
    fn write_map_begin(&mut self, identifier: &TMapIdentifier) {
        let key_type = identifier.key_type;
        self.write_byte(key_type.into());
        let val_type = identifier.value_type;
        self.write_byte(val_type.into());
        self.attachment.push(self.trans.len());
        self.write_i32(identifier.size as i32);
    }

    #[inline]
    fn write_map_end(&mut self, len: usize) {
        self.write_length(len)
    }

    #[inline]
    fn write_byte(&mut self, b: u8) {
        self.trans.put_u8(b);
    }

    #[inline]
    fn write_bool(&mut self, b: bool) {
        self.trans.put_i8(if b { 1 } else { 0 });
    }

    #[inline]
    fn write_i8(&mut self, i: i8) {
        self.trans.put_i8(i);
    }

    #[inline]
    fn write_i16(&mut self, i: i16) {
        self.trans.put_i16(i);
    }

    #[inline]
    fn write_i32(&mut self, i: i32) {
        self.trans.put_i32(i);
    }

    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.trans.put_i64(i);
    }

    #[inline]
    fn write_double(&mut self, d: f64) {
        self.trans.put_f64(d);
    }

    #[inline]
    fn write_uuid(&mut self, u: [u8; 16]) {
        self.trans.put_slice(&u);
    }

    #[inline]
    fn write_bytes(&mut self, b: &[u8]) {
        self.write_i32(b.len() as i32);
        self.trans.put_slice(b);
    }

    #[inline]
    fn write_string(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
    }

    #[inline(always)]
    fn flush(&mut self) {}

    #[inline]
    fn buf(&mut self) -> &mut Self::Buf {
        self.trans
    }
}
