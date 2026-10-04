#![forbid(unsafe_code)]
#![allow(clippy::double_must_use)]

#[cfg(feature = "file-descriptor-set")]
pub use kvdb::FILE_DESCRIPTOR_SET;

mod error;

pub use error::{ErrorKind, decode_error_details, encode_error_details};

pub mod kvdb;

mod value;
pub use value::{optional_value, required_value, scan_entries, value_from_proto, value_to_proto};
