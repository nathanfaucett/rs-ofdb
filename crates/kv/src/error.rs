use core::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    InvalidDocument,
    TypeMismatch,
    Conflict,
    CommitFailed,
    RollbackFailed,
    UnsupportedOperation,
    Storage,
    Internal,
}

#[cfg(feature = "remote")]
impl From<kv_proto::ErrorKind> for ErrorKind {
    fn from(kind: kv_proto::ErrorKind) -> Self {
        match kind {
            kv_proto::ErrorKind::InvalidDocument => Self::InvalidDocument,
            kv_proto::ErrorKind::TypeMismatch => Self::TypeMismatch,
            kv_proto::ErrorKind::Conflict => Self::Conflict,
            kv_proto::ErrorKind::CommitFailed => Self::CommitFailed,
            kv_proto::ErrorKind::RollbackFailed => Self::RollbackFailed,
            kv_proto::ErrorKind::UnsupportedOperation => Self::UnsupportedOperation,
            kv_proto::ErrorKind::Storage => Self::Storage,
            kv_proto::ErrorKind::Internal => Self::Internal,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    #[cfg(not(any(
        feature = "remote",
        feature = "in-memory",
        feature = "redb",
        feature = "sync",
        feature = "server"
    )))]
    Unconfigured,
    #[cfg(any(feature = "in-memory", feature = "redb", feature = "sync"))]
    Storage { kind: ErrorKind, message: String },
    #[cfg(feature = "remote")]
    Connect(String),
    #[cfg(feature = "remote")]
    Transport(String),
    #[cfg(feature = "remote")]
    Query { kind: ErrorKind, message: String },
    #[cfg(feature = "remote")]
    Timeout,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(not(any(
                feature = "remote",
                feature = "in-memory",
                feature = "redb",
                feature = "sync",
                feature = "server"
            )))]
            Self::Unconfigured => formatter.write_str("KV facade has no enabled features"),
            #[cfg(any(feature = "in-memory", feature = "redb", feature = "sync"))]
            Self::Storage { kind, message } => {
                write!(formatter, "KV storage error ({kind:?}): {message}")
            }
            #[cfg(feature = "remote")]
            Self::Connect(message) => write!(formatter, "KV connection error: {message}"),
            #[cfg(feature = "remote")]
            Self::Transport(message) => write!(formatter, "KV transport error: {message}"),
            #[cfg(feature = "remote")]
            Self::Query { kind, message } => {
                write!(formatter, "KV query error ({kind:?}): {message}")
            }
            #[cfg(feature = "remote")]
            Self::Timeout => formatter.write_str("KV request deadline exceeded"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

#[cfg(any(feature = "in-memory", feature = "redb", feature = "sync"))]
impl From<btree::BTreeError> for Error {
    fn from(error: btree::BTreeError) -> Self {
        use btree::BTreeError;

        let kind = match &error {
            BTreeError::InvalidDocument => ErrorKind::InvalidDocument,
            BTreeError::TypeMismatch => ErrorKind::TypeMismatch,
            BTreeError::Conflict => ErrorKind::Conflict,
            BTreeError::CommitFailed => ErrorKind::CommitFailed,
            BTreeError::RollbackFailed => ErrorKind::RollbackFailed,
            BTreeError::UnsupportedOperation => ErrorKind::UnsupportedOperation,
            BTreeError::Custom(_) => ErrorKind::Storage,
        };
        Self::Storage {
            kind,
            message: error.to_string(),
        }
    }
}
