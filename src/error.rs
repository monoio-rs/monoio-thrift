use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct CodecError {
    pub kind: CodecErrorKind,
    pub message: Cow<'static, str>,
}

impl CodecError {
    pub fn new<S: Into<Cow<'static, str>>>(kind: CodecErrorKind, message: S) -> CodecError {
        CodecError {
            message: message.into(),
            kind,
        }
    }

    pub const fn invalid_data() -> CodecError {
        CodecError {
            message: Cow::Borrowed("invalid data"),
            kind: CodecErrorKind::InvalidData,
        }
    }
}

impl Display for CodecError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use CodecErrorKind::*;

        write!(f, "{}", self.message)?;
        if !matches!(
            self.kind,
            BadVersion | InvalidData | NegativeSize | NotImplemented | UnknownMethod
        ) {
            write!(f, ", caused by {}", self.kind)?;
        }
        Ok(())
    }
}

impl std::error::Error for CodecError {}

impl From<std::io::Error> for CodecError {
    fn from(value: std::io::Error) -> Self {
        CodecError::new(CodecErrorKind::IOError(value), "")
    }
}

#[derive(Debug)]
pub enum CodecErrorKind {
    InvalidData,
    NegativeSize,
    BadVersion,
    NotImplemented,
    DepthLimit,
    UnknownMethod,
    IOError(std::io::Error),
}

impl Display for CodecErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CodecErrorKind::IOError(e) => write!(f, "IOError: {}", e),
            CodecErrorKind::InvalidData => write!(f, "InvalidData"),
            CodecErrorKind::NegativeSize => write!(f, "NegativeSize"),
            CodecErrorKind::BadVersion => write!(f, "BadVersion"),
            CodecErrorKind::NotImplemented => write!(f, "NotImplemented"),
            CodecErrorKind::DepthLimit => write!(f, "DepthLimit"),
            CodecErrorKind::UnknownMethod => write!(f, "UnknownMethod"),
        }
    }
}
