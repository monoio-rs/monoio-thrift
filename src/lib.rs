#![feature(utf8_chunks)]

pub mod codec;

mod error;

pub use error::{CodecError, CodecErrorKind};

pub mod protocol;

pub mod thrift;

pub mod binary;
