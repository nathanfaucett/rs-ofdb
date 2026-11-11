mod protocol;
mod subscription;
mod transport;

#[cfg(feature = "iroh")]
mod iroh_transport;

pub use protocol::{
    AckConfig, Error, HelloConfig, MAX_MESSAGE_BYTES, PushConfig, PushMessage, PushResult,
    decode_message, encode_message,
};
pub use transport::PushSyncTransport;

#[cfg(all(feature = "std", not(target_arch = "wasm32")))]
mod sync;

#[cfg(all(feature = "std", not(target_arch = "wasm32")))]
pub use sync::{PushSynchronizeConfig, PushSynchronizeError, push_synchronize};

#[cfg(all(feature = "std", target_arch = "wasm32"))]
mod wasm_sync;

#[cfg(all(feature = "std", target_arch = "wasm32"))]
pub use wasm_sync::{PushSynchronizeConfig, PushSynchronizeError, push_synchronize};

#[cfg(feature = "iroh")]
pub use iroh_transport::IrohTransport;

pub use btree::ChangeEvent;
/// Re-export ChangeSubscription from btree for convenience.
pub use btree::ChangeSubscription;
