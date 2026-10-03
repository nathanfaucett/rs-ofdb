use alloc::string::String;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryErrorKind {
    Validation = 1,
    Rejected = 2,
    Unsupported = 3,
    Storage = 4,
    Internal = 5,
    Transport = 6,
    Protocol = 7,
    Timeout = 8,
}

impl QueryErrorKind {
    pub const fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Validation),
            2 => Some(Self::Rejected),
            3 => Some(Self::Unsupported),
            4 => Some(Self::Storage),
            5 => Some(Self::Internal),
            6 => Some(Self::Transport),
            7 => Some(Self::Protocol),
            8 => Some(Self::Timeout),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryError {
    pub kind: QueryErrorKind,
    pub message: String,
}

impl QueryError {
    pub fn new(kind: QueryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl core::error::Error for QueryError {}

impl core::fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}
