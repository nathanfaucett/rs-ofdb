tonic::include_proto!("kvdb.v2");

#[cfg(feature = "file-descriptor-set")]
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("kv");
