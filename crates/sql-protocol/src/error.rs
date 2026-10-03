use alloc::{string::String, vec::Vec};
use query::QueryErrorKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryServiceError {
    Invalid(String),
    Rejected(String),
    Unsupported(&'static str),
    Storage(String),
    Internal(String),
}

pub fn encode_error_detail(kind: QueryErrorKind, message: &str) -> Vec<u8> {
    let mut detail = Vec::with_capacity(message.len() + 1);
    detail.push(kind as u8);
    detail.extend_from_slice(message.as_bytes());
    detail
}

pub fn decode_error_detail(detail: &[u8]) -> Option<(QueryErrorKind, String)> {
    let (&kind, message) = detail.split_first()?;
    let kind = QueryErrorKind::from_wire(kind)?;
    let message = core::str::from_utf8(message).ok()?.into();
    Some((kind, message))
}

impl core::fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid request: {message}"),
            Self::Rejected(message) => write!(formatter, "request rejected: {message}"),
            Self::Unsupported(message) => write!(formatter, "unsupported: {message}"),
            Self::Storage(message) => write!(formatter, "storage error: {message}"),
            Self::Internal(message) => write!(formatter, "internal error: {message}"),
        }
    }
}
