#![forbid(unsafe_code)]
#![allow(clippy::double_must_use)]

#[cfg(feature = "file-descriptor-set")]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("db");

tonic::include_proto!("db");
