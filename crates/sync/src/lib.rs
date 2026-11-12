mod protocol;
mod subscription;
mod transport;

#[cfg(feature = "iroh")]
mod iroh_transport;

pub use protocol::{
    AckConfig, Config, Error, HelloConfig, MAX_MESSAGE_BYTES, Message, SyncResult, decode_message,
    encode_message,
};
pub use transport::SyncTransport;

#[cfg(all(feature = "std", not(target_arch = "wasm32")))]
mod sync;

#[cfg(all(feature = "std", not(target_arch = "wasm32")))]
pub use sync::{SynchronizeConfig, SynchronizeError, synchronize};

#[cfg(all(feature = "std", target_arch = "wasm32"))]
mod wasm_sync;

#[cfg(all(feature = "std", target_arch = "wasm32"))]
pub use wasm_sync::{SynchronizeConfig, SynchronizeError, synchronize};

#[cfg(feature = "iroh")]
pub use iroh_transport::IrohTransport;

pub use btree::ChangeEvent;
/// Re-export ChangeSubscription from btree for convenience.
pub use btree::ChangeSubscription;
