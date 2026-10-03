use crate::{Error, ErrorKind, client::map_status};
use proto_kv::{ErrorKind as ProtoErrorKind, encode_error_details};
use tonic::{Code, Status, codegen::Bytes};

#[test]
fn remote_status_keeps_structured_store_category() {
    let details = Bytes::from(encode_error_details(
        ProtoErrorKind::Conflict,
        "divergent history",
    ));
    let status = Status::with_details(Code::Internal, "different message", details);
    assert_eq!(
        map_status(status),
        Error::Query {
            kind: ErrorKind::Conflict,
            message: "divergent history".to_owned(),
        }
    );
}

#[test]
fn generic_remote_statuses_remain_distinguishable() {
    assert_eq!(
        map_status(Status::new(Code::DeadlineExceeded, "late")),
        Error::Timeout
    );
    assert_eq!(
        map_status(Status::new(Code::Unavailable, "offline")),
        Error::Transport("offline".to_owned())
    );
}

#[cfg(any(feature = "in-memory", feature = "redb", feature = "sync"))]
#[test]
fn local_store_errors_keep_their_category() {
    assert_eq!(
        Error::from(btree::BTreeError::Conflict),
        Error::Storage {
            kind: ErrorKind::Conflict,
            message: "Conflict".to_owned(),
        }
    );
}
