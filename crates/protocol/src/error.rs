#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryServiceError {
    Invalid(String),
    Rejected(String),
    Unsupported(&'static str),
    Internal(String),
}

impl core::fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid request: {message}"),
            Self::Rejected(message) => write!(formatter, "request rejected: {message}"),
            Self::Unsupported(message) => write!(formatter, "unsupported: {message}"),
            Self::Internal(message) => write!(formatter, "internal error: {message}"),
        }
    }
}
