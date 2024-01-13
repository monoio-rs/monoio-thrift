use bytes::Bytes;

use crate::thrift::{
    TFieldIdentifier, TListIdentifier, TMapIdentifier, TMessageIdentifier, TSetIdentifier,
    TStructIdentifier, TType,
};
use crate::CodecError;

/// TInputProtocol is for the protocol that the total payload length
/// can be known with low cost. For example, message with FramedHeader
/// TTHeader or MeshHeader.
/// All output types are Copy or borrow of raw bytes.
pub trait TInputProtocol<'x> {
    type Buf<'b>
    where
        Self: 'b;
    /// Read the beginning of a Thrift message.
    fn read_message_begin(&mut self) -> Result<TMessageIdentifier, CodecError>;
    /// Read the end of a Thrift message.
    fn read_message_end(&mut self) -> Result<(), CodecError>;
    /// Read the beginning of a Thrift struct.
    fn read_struct_begin(&mut self) -> Result<TStructIdentifier, CodecError>;
    /// Read the end of a Thrift struct.
    fn read_struct_end(&mut self) -> Result<(), CodecError>;
    /// Read the beginning of a Thrift struct field.
    fn read_field_begin(&mut self) -> Result<TFieldIdentifier, CodecError>;
    /// Read the end of a Thrift struct field.
    fn read_field_end(&mut self) -> Result<(), CodecError>;
    /// Read the beginning of a list.
    fn read_list_begin(&mut self) -> Result<TListIdentifier, CodecError>;
    /// Read the end of a list.
    fn read_list_end(&mut self) -> Result<(), CodecError>;
    /// Read the beginning of a set.
    fn read_set_begin(&mut self) -> Result<TSetIdentifier, CodecError>;
    /// Read the end of a set.
    fn read_set_end(&mut self) -> Result<(), CodecError>;
    /// Read the beginning of a map.
    fn read_map_begin(&mut self) -> Result<TMapIdentifier, CodecError>;
    /// Read the end of a map.
    fn read_map_end(&mut self) -> Result<(), CodecError>;
    /// Read an unsigned byte.
    fn read_byte(&mut self) -> Result<u8, CodecError>;
    /// Read a bool.
    fn read_bool(&mut self) -> Result<bool, CodecError>;
    /// Read a word.
    fn read_i8(&mut self) -> Result<i8, CodecError>;
    /// Read a 16-bit signed integer.
    fn read_i16(&mut self) -> Result<i16, CodecError>;
    /// Read a 32-bit signed integer.
    fn read_i32(&mut self) -> Result<i32, CodecError>;
    /// Read a 64-bit signed integer.
    fn read_i64(&mut self) -> Result<i64, CodecError>;
    /// Read a 64-bit float.
    fn read_double(&mut self) -> Result<f64, CodecError>;
    /// Read a uuid.
    fn read_uuid(&mut self) -> Result<[u8; 16], CodecError>;
    /// Read a binary.
    fn read_bytes(&mut self) -> Result<&'x [u8], CodecError>;
    /// Read a fixed-length string.
    fn read_string(&mut self) -> Result<&'x str, CodecError>;
    /// Skip a field.
    fn skip_field(&mut self, ttype: TType) -> Result<(), CodecError>;

    fn buf<'a>(&'a mut self) -> &'a mut Self::Buf<'x>
    where
        'x: 'a;
}

macro_rules! async_fn {
    (async fn $fname:ident(&mut self $(,$arg:ident: $arg_type:ty)*) -> Result<$futname:ident($out:ty)>) => {
        fn $fname(&mut self $(,$arg : $arg_type)*) -> impl std::future::Future<Output = Result<$out, CodecError>>;
    };
    ($(async fn $fname:ident(&mut self $(,$arg:ident: $arg_type:ty)*) -> Result<$futname:ident($out:ty)>;)*) => {
        $(async_fn!(async fn $fname(&mut self $(,$arg : $arg_type)*) -> Result<$futname($out)>);)*
    };
}

pub trait TAsyncSkipProtocol {
    async_fn! {
        async fn skip_message(&mut self) -> Result<SkipMessage(())>;
        async fn skip_field(&mut self, ttype: TType) -> Result<SkipField(())>;
    }
}

/// TAsyncInputProtocol is for the protocol that the total payload length
/// cannot be known with low cost. For example, raw binary message.
/// If the data is insufficient, the impl will read with the io inside itself.
/// To archive best performance, the impl may use Bytes to avoid memory copy.
pub trait TAsyncInputProtocol {
    async_fn! {
        async fn read_message_begin(&mut self) -> Result<ReadMessageBegin(TMessageIdentifier<'static>)>;
        async fn read_message_end(&mut self) -> Result<ReadMessageEnd(())>;
        async fn read_struct_begin(&mut self) -> Result<ReadStructBegin(TStructIdentifier)>;
        async fn read_struct_end(&mut self) -> Result<ReadStructEnd(())>;
        async fn read_field_begin(&mut self) -> Result<ReadFieldBegin(TFieldIdentifier)>;
        async fn read_field_end(&mut self) -> Result<ReadFieldEnd(())>;
        async fn read_list_begin(&mut self) -> Result<ReadListBegin(TListIdentifier)>;
        async fn read_list_end(&mut self) -> Result<ReadListEnd(())>;
        async fn read_set_begin(&mut self) -> Result<ReadSetBegin(TSetIdentifier)>;
        async fn read_set_end(&mut self) -> Result<ReadSetEnd(())>;
        async fn read_map_begin(&mut self) -> Result<ReadMapBegin(TMapIdentifier)>;
        async fn read_map_end(&mut self) -> Result<ReadMapEnd(())>;
        async fn read_byte(&mut self) -> Result<ReadByte(u8)>;
        async fn read_bool(&mut self) -> Result<ReadBool(bool)>;
        async fn read_i8(&mut self) -> Result<ReadI8(i8)>;
        async fn read_i16(&mut self) -> Result<ReadI16(i16)>;
        async fn read_i32(&mut self) -> Result<ReadI32(i32)>;
        async fn read_i64(&mut self) -> Result<ReadI64(i64)>;
        async fn read_double(&mut self) -> Result<ReadDouble(f64)>;
        async fn read_uuid(&mut self) -> Result<ReadUuid([u8; 16])>;
        async fn read_bytes(&mut self) -> Result<ReadBytes(Bytes)>;
        async fn read_string(&mut self) -> Result<ReadString(Bytes)>;
    }
}

pub trait TOutputProtocol {
    type Buf;

    /// Write the beginning of a Thrift message.
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier);
    /// Write the end of a Thrift message.
    fn write_message_end(&mut self);
    /// Write the beginning of a Thrift struct.
    fn write_struct_begin(&mut self, identifier: &TStructIdentifier);
    /// Write the end of a Thrift struct.
    fn write_struct_end(&mut self);
    /// Write the beginning of a Thrift field.
    fn write_field_begin(&mut self, field_type: TType, id: i16);
    /// Write the end of a Thrift field.
    fn write_field_end(&mut self);
    /// Write a STOP field indicating that all the fields in a struct have been
    /// written.
    fn write_field_stop(&mut self);
    /// Write the beginning of a list.
    fn write_list_begin(&mut self, identifier: &TListIdentifier);
    /// Write the end of a list.
    fn write_list_end(&mut self, len: usize);
    /// Write the beginning of a set.
    fn write_set_begin(&mut self, identifier: &TSetIdentifier);
    /// Write the end of a set.
    fn write_set_end(&mut self, len: usize);
    /// Write the beginning of a map.
    fn write_map_begin(&mut self, identifier: &TMapIdentifier);
    /// Write the end of a map.
    fn write_map_end(&mut self, len: usize);
    /// Write a byte.
    fn write_byte(&mut self, b: u8);
    /// Write a bool.
    fn write_bool(&mut self, b: bool);
    /// Write an 8-bit signed integer.
    fn write_i8(&mut self, i: i8);
    /// Write a 16-bit signed integer.
    fn write_i16(&mut self, i: i16);
    /// Write a 32-bit signed integer.
    fn write_i32(&mut self, i: i32);
    /// Write a 64-bit signed integer.
    fn write_i64(&mut self, i: i64);
    /// Write a 64-bit float.
    fn write_double(&mut self, d: f64);
    /// Write a uuid.
    fn write_uuid(&mut self, u: [u8; 16]);
    /// Write a fixed-length byte array.
    fn write_bytes(&mut self, b: &[u8]);
    /// Write a fixed-length string.
    fn write_string(&mut self, s: &str);

    /// Flush buffered bytes to the underlying transport.
    fn flush(&mut self);
    fn buf(&mut self) -> &mut Self::Buf;
}

impl<T: TOutputProtocol> TOutputProtocol for &mut T {
    type Buf = T::Buf;

    #[inline]
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) {
        (**self).write_message_begin(identifier)
    }
    #[inline(always)]
    fn write_message_end(&mut self) {
        (**self).write_message_end()
    }
    #[inline]
    fn write_struct_begin(&mut self, identifier: &TStructIdentifier) {
        (**self).write_struct_begin(identifier)
    }
    #[inline(always)]
    fn write_struct_end(&mut self) {
        (**self).write_struct_end()
    }
    #[inline]
    fn write_field_begin(&mut self, field_type: TType, id: i16) {
        (**self).write_field_begin(field_type, id)
    }
    #[inline(always)]
    fn write_field_end(&mut self) {
        (**self).write_field_end()
    }
    #[inline]
    fn write_field_stop(&mut self) {
        (**self).write_field_stop()
    }
    #[inline]
    fn write_list_begin(&mut self, identifier: &TListIdentifier) {
        (**self).write_list_begin(identifier)
    }
    #[inline]
    fn write_list_end(&mut self, len: usize) {
        (**self).write_list_end(len)
    }
    #[inline]
    fn write_set_begin(&mut self, identifier: &TSetIdentifier) {
        (**self).write_set_begin(identifier)
    }
    #[inline]
    fn write_set_end(&mut self, len: usize) {
        (**self).write_set_end(len)
    }
    #[inline]
    fn write_map_begin(&mut self, identifier: &TMapIdentifier) {
        (**self).write_map_begin(identifier)
    }
    #[inline]
    fn write_map_end(&mut self, len: usize) {
        (**self).write_map_end(len)
    }
    #[inline]
    fn write_byte(&mut self, b: u8) {
        (**self).write_byte(b)
    }
    #[inline]
    fn write_bool(&mut self, b: bool) {
        (**self).write_bool(b)
    }

    #[inline]
    fn write_i8(&mut self, i: i8) {
        (**self).write_i8(i)
    }
    #[inline]
    fn write_i16(&mut self, i: i16) {
        (**self).write_i16(i)
    }
    #[inline]
    fn write_i32(&mut self, i: i32) {
        (**self).write_i32(i)
    }
    #[inline]
    fn write_i64(&mut self, i: i64) {
        (**self).write_i64(i)
    }
    #[inline]
    fn write_double(&mut self, d: f64) {
        (**self).write_double(d)
    }
    #[inline]
    fn write_uuid(&mut self, u: [u8; 16]) {
        (**self).write_uuid(u)
    }
    #[inline]
    fn write_bytes(&mut self, b: &[u8]) {
        (**self).write_bytes(b)
    }
    #[inline]
    fn write_string(&mut self, s: &str) {
        (**self).write_string(s)
    }
    #[inline(always)]
    fn flush(&mut self) {
        (**self).flush()
    }
    #[inline]
    fn buf(&mut self) -> &mut Self::Buf {
        (**self).buf()
    }
}

pub trait TLengthProtocol {
    fn message_begin_len(identifier: &TMessageIdentifier) -> usize;
    fn message_end_len() -> usize;
    fn struct_begin_len(identifier: &TStructIdentifier) -> usize;
    fn struct_end_len() -> usize;
    fn field_begin_len(field_type: TType, id: Option<i16>) -> usize;
    fn field_end_len() -> usize;
    fn field_stop_len() -> usize;
    fn bool_len(b: bool) -> usize;
    fn bytes_len(b: &[u8]) -> usize;
    fn bytes_vec_len(b: &[u8]) -> usize;
    fn byte_len(b: u8) -> usize;
    fn uuid_len(u: [u8; 16]) -> usize;
    fn i8_len(i: i8) -> usize;
    fn i16_len(i: i16) -> usize;
    fn i32_len(i: i32) -> usize;
    fn i64_len(i: i64) -> usize;
    fn double_len(d: f64) -> usize;
    fn string_len(s: &str) -> usize;
    fn list_begin_len(identifier: TListIdentifier) -> usize;
    fn list_end_len() -> usize;
    fn set_begin_len(identifier: TSetIdentifier) -> usize;
    fn set_end_len() -> usize;
    fn map_begin_len(identifier: TMapIdentifier) -> usize;
    fn map_end_len() -> usize;
}

impl<T: TLengthProtocol> TLengthProtocol for &mut T {
    #[inline]
    fn message_begin_len(identifier: &TMessageIdentifier) -> usize {
        T::message_begin_len(identifier)
    }
    #[inline]
    fn message_end_len() -> usize {
        T::message_end_len()
    }
    #[inline]
    fn struct_begin_len(identifier: &TStructIdentifier) -> usize {
        T::struct_begin_len(identifier)
    }
    #[inline]
    fn struct_end_len() -> usize {
        T::struct_end_len()
    }
    #[inline]
    fn field_begin_len(field_type: TType, id: Option<i16>) -> usize {
        T::field_begin_len(field_type, id)
    }
    #[inline]
    fn field_end_len() -> usize {
        T::field_end_len()
    }
    #[inline]
    fn field_stop_len() -> usize {
        T::field_stop_len()
    }
    #[inline]
    fn bool_len(b: bool) -> usize {
        T::bool_len(b)
    }
    #[inline]
    fn bytes_len(b: &[u8]) -> usize {
        T::bytes_len(b)
    }
    #[inline]
    fn bytes_vec_len(b: &[u8]) -> usize {
        T::bytes_vec_len(b)
    }
    #[inline]
    fn byte_len(b: u8) -> usize {
        T::byte_len(b)
    }
    #[inline]
    fn uuid_len(u: [u8; 16]) -> usize {
        T::uuid_len(u)
    }
    #[inline]
    fn i8_len(i: i8) -> usize {
        T::i8_len(i)
    }
    #[inline]
    fn i16_len(i: i16) -> usize {
        T::i16_len(i)
    }
    #[inline]
    fn i32_len(i: i32) -> usize {
        T::i32_len(i)
    }
    #[inline]
    fn i64_len(i: i64) -> usize {
        T::i64_len(i)
    }
    #[inline]
    fn double_len(d: f64) -> usize {
        T::double_len(d)
    }
    #[inline]
    fn string_len(s: &str) -> usize {
        T::string_len(s)
    }
    #[inline]
    fn list_begin_len(identifier: TListIdentifier) -> usize {
        T::list_begin_len(identifier)
    }
    #[inline]
    fn list_end_len() -> usize {
        T::list_end_len()
    }
    #[inline]
    fn set_begin_len(identifier: TSetIdentifier) -> usize {
        T::set_begin_len(identifier)
    }
    #[inline]
    fn set_end_len() -> usize {
        T::set_end_len()
    }
    #[inline]
    fn map_begin_len(identifier: TMapIdentifier) -> usize {
        T::map_begin_len(identifier)
    }
    #[inline]
    fn map_end_len() -> usize {
        T::map_end_len()
    }
}
