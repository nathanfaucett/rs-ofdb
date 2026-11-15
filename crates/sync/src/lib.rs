#![allow(dead_code, reason = "public API crate - items exported for consumers")]

mod protocol;
mod subscription;
mod transport;

#[cfg(feature = "iroh")]
mod iroh_transport;

pub use protocol::{
    AckConfig, Config, Error, HelloConfig, MAX_MESSAGE_BYTES, Message, SyncResult, SyncRole,
    decode_message, encode_message,
};
pub use transport::SyncTransport;

#[cfg(all(feature = "std", not(feature = "wasm")))]
mod sync;

#[cfg(all(feature = "std", not(feature = "wasm")))]
pub use sync::{SynchronizeConfig, SynchronizeError, synchronize};

#[cfg(feature = "wasm")]
mod wasm_sync;

#[cfg(feature = "wasm")]
pub use wasm_sync::{
    PushConfig, PushMessage, PushResult, PushSyncTransport, PushSynchronizeConfig,
    PushSynchronizeError, SynchronizeConfig, SynchronizeError, synchronize,
};

#[cfg(feature = "iroh")]
pub use iroh_transport::IrohTransport;

pub use btree::ChangeEvent;
/// Re-export `ChangeSubscription` from btree for convenience.
pub use btree::ChangeSubscription;
