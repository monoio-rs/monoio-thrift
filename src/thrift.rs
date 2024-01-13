use std::str::from_utf8_unchecked;

use crate::{CodecError, CodecErrorKind};

/// Thrift struct identifier.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct TStructIdentifier {
    /// Name of the encoded Thrift struct.
    pub name: Option<&'static str>,
}

impl TStructIdentifier {
    /// Create a `TStructIdentifier` for a struct named `name`.
    pub const fn new(name: Option<&'static str>) -> TStructIdentifier {
        TStructIdentifier { name }
    }
}

/// Thrift types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TType {
    Stop = 0,
    Void = 1,
    Bool = 2,
    I8 = 3,
    Double = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    Binary = 11, // 0xb
    Struct = 12, // 0xc
    Map = 13,    // 0xd
    Set = 14,    // 0xe
    List = 15,   // 0xf
    Uuid = 16,   // 0x10
}

impl From<TType> for u8 {
    #[inline]
    fn from(ttype: TType) -> Self {
        ttype as u8
    }
}

impl TryFrom<u8> for TType {
    type Error = CodecError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(TType::Stop),
            1 => Ok(TType::Void),
            2 => Ok(TType::Bool),
            3 => Ok(TType::I8),
            4 => Ok(TType::Double),
            6 => Ok(TType::I16),
            8 => Ok(TType::I32),
            10 => Ok(TType::I64),
            11 => Ok(TType::Binary),
            12 => Ok(TType::Struct),
            13 => Ok(TType::Map),
            14 => Ok(TType::Set),
            15 => Ok(TType::List),
            16 => Ok(TType::Uuid),
            _ => Err(CodecError::new(
                CodecErrorKind::InvalidData,
                format!("invalid ttype {}", value),
            )),
        }
    }
}

/// Thrift message types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TMessageType {
    /// Service-call request.
    Call = 1,
    /// Service-call response.
    Reply = 2,
    /// Unexpected error in the remote service.
    Exception = 3,
    /// One-way service-call request (no response is expected).
    OneWay = 4,
}

impl TryFrom<u8> for TMessageType {
    type Error = CodecError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TMessageType::Call),
            2 => Ok(TMessageType::Reply),
            3 => Ok(TMessageType::Exception),
            4 => Ok(TMessageType::OneWay),
            _ => Err(CodecError::new(
                CodecErrorKind::InvalidData,
                format!("invalid tmessage type {}", value),
            )),
        }
    }
}

impl From<TMessageType> for u8 {
    fn from(t: TMessageType) -> Self {
        t as u8
    }
}

#[derive(Debug)]
pub enum CowBytes<'a, T: ?Sized> {
    Borrowed(&'a T),
    Owned(bytes::Bytes),
}

impl<'a> CowBytes<'a, str> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CowBytes::Borrowed(s) => s.as_bytes(),
            CowBytes::Owned(b) => b,
        }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            CowBytes::Borrowed(s) => s,
            CowBytes::Owned(b) => unsafe { from_utf8_unchecked(b.as_ref()) },
        }
    }
}

impl<'a> CowBytes<'a, [u8]> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CowBytes::Borrowed(s) => s,
            CowBytes::Owned(b) => b,
        }
    }
}

impl<'a, T: ?Sized> Clone for CowBytes<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed(arg0) => Self::Borrowed(arg0),
            Self::Owned(arg0) => Self::Owned(arg0.clone()),
        }
    }
}

impl<'a> PartialEq for CowBytes<'a, str> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl<'a> Eq for CowBytes<'a, str> {}
impl<'a> PartialEq for CowBytes<'a, [u8]> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl<'a> Eq for CowBytes<'a, [u8]> {}

/// Thrift message identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TMessageIdentifier<'a> {
    /// Service call the message is associated with.
    pub name: CowBytes<'a, str>,
    /// Message type.
    pub message_type: TMessageType,
    /// Ordered sequence number identifying the message.
    pub sequence_number: i32,
}

impl<'a> TMessageIdentifier<'a> {
    /// Create a `TMessageIdentifier` for a Thrift service-call named `name`
    /// with message type `message_type` and sequence number `sequence_number`.
    pub const fn new(
        name: CowBytes<'a, str>,
        message_type: TMessageType,
        sequence_number: i32,
    ) -> TMessageIdentifier {
        TMessageIdentifier {
            name,
            message_type,
            sequence_number,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct TListIdentifier {
    /// Type of the elements in the list.
    pub element_type: TType,
    /// Number of elements in the list.
    pub size: usize,
}

impl TListIdentifier {
    /// Create a `TListIdentifier` for a list with `size` elements of type
    /// `element_type`.
    pub const fn new(element_type: TType, size: usize) -> TListIdentifier {
        TListIdentifier { element_type, size }
    }
}

/// Thrift set identifier.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct TSetIdentifier {
    /// Type of the elements in the set.
    pub element_type: TType,
    /// Number of elements in the set.
    pub size: usize,
}

impl TSetIdentifier {
    /// Create a `TSetIdentifier` for a set with `size` elements of type
    /// `element_type`.
    pub const fn new(element_type: TType, size: usize) -> TSetIdentifier {
        TSetIdentifier { element_type, size }
    }
}

/// Thrift field identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TFieldIdentifier {
    /// Name of the Thrift field.
    ///
    /// `None` if it's not sent over the wire.
    pub name: Option<&'static str>,
    /// Field type.
    ///
    /// This may be a primitive, container, or a struct.
    pub field_type: TType,
    /// Thrift field id.
    ///
    /// `None` only if `field_type` is `TType::Stop`.
    pub id: Option<i16>,
}

impl TFieldIdentifier {
    /// Create a `TFieldIdentifier` for a field named `name` with type
    /// `field_type` and field id `id`.
    ///
    /// `id` should be `None` if `field_type` is `TType::Stop`.
    pub const fn new(
        name: Option<&'static str>,
        field_type: TType,
        id: Option<i16>,
    ) -> TFieldIdentifier {
        TFieldIdentifier {
            name,
            field_type,
            id,
        }
    }
}

/// Thrift map identifier.
#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub struct TMapIdentifier {
    /// Map key type.
    pub key_type: TType,
    /// Map value type.
    pub value_type: TType,
    /// Number of entries in the map.
    pub size: usize,
}

impl TMapIdentifier {
    /// Create a `TMapIdentifier` for a map with `size` entries of type
    /// `key_type -> value_type`.
    pub const fn new(key_type: TType, value_type: TType, size: usize) -> TMapIdentifier {
        TMapIdentifier {
            key_type,
            value_type,
            size,
        }
    }
}
