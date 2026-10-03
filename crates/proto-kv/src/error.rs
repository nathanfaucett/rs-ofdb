const ERROR_PREFIX: &[u8] = b"ofdb-kv-error\0\x01";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ErrorKind {
    InvalidDocument = 1,
    TypeMismatch = 2,
    Conflict = 3,
    CommitFailed = 4,
    RollbackFailed = 5,
    UnsupportedOperation = 6,
    Storage = 7,
    Internal = 8,
}

impl TryFrom<u8> for ErrorKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::InvalidDocument),
            2 => Ok(Self::TypeMismatch),
            3 => Ok(Self::Conflict),
            4 => Ok(Self::CommitFailed),
            5 => Ok(Self::RollbackFailed),
            6 => Ok(Self::UnsupportedOperation),
            7 => Ok(Self::Storage),
            8 => Ok(Self::Internal),
            _ => Err(()),
        }
    }
}

pub fn encode_error_details(kind: ErrorKind, message: &str) -> Vec<u8> {
    let mut details = Vec::with_capacity(ERROR_PREFIX.len() + 1 + message.len());
    details.extend_from_slice(ERROR_PREFIX);
    details.push(kind as u8);
    details.extend_from_slice(message.as_bytes());
    details
}

pub fn decode_error_details(details: &[u8]) -> Option<(ErrorKind, &str)> {
    let payload = details.strip_prefix(ERROR_PREFIX)?;
    let (&kind, message) = payload.split_first()?;
    let kind = ErrorKind::try_from(kind).ok()?;
    let message = core::str::from_utf8(message).ok()?;
    Some((kind, message))
}

#[cfg(test)]
mod tests {
    use super::{ERROR_PREFIX, ErrorKind, decode_error_details, encode_error_details};

    #[test]
    fn structured_error_details_round_trip() {
        let bytes = encode_error_details(ErrorKind::Conflict, "divergent history");
        assert_eq!(
            decode_error_details(&bytes),
            Some((ErrorKind::Conflict, "divergent history"))
        );
    }

    #[test]
    fn invalid_error_details_are_rejected() {
        assert_eq!(decode_error_details(b"not an ofdb error"), None);
        let mut details = encode_error_details(ErrorKind::Conflict, "bad");
        details[ERROR_PREFIX.len()] = 255;
        assert_eq!(decode_error_details(&details), None);
    }
}
