#![forbid(unsafe_code)]
#![allow(clippy::double_must_use)]

#[cfg(feature = "file-descriptor-set")]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("kv");

mod error;

pub use error::{ErrorKind, decode_error_details, encode_error_details};

pub mod kvdb;
