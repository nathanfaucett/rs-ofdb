#![no_std]

extern crate alloc;
#[cfg(feature = "iroh")]
extern crate std;

mod codec;
#[cfg(feature = "iroh")]
mod iroh_transport;
mod protocol;
mod session;
mod staging;
mod state;
mod transport;

pub use codec::SyncRowCodec;
#[cfg(feature = "iroh")]
pub use iroh_transport::IrohTransport;
pub use protocol::{
    PROTOCOL_VERSION, SyncHello, SyncIncrementalChange, SyncMessage, SyncRowInventory,
    SyncSnapshotRequest,
};
pub use session::{
    DEFAULT_MAX_SESSION_BYTES, MAX_APPLY_BATCH_BYTES, MAX_MESSAGE_BYTES, SessionConfig, SyncError,
    SyncResult, SyncRole, apply_incremental_changes_for, apply_sync_state_batch_for,
    apply_sync_state_for, export_sync_state_for, sync_manifest_for, synchronize,
    synchronize_with_stage_factory,
};
pub use staging::{MemorySyncStageFactory, SyncStage, SyncStageFactory};
pub use state::{StateDigest, SyncChangeId, SyncKey, SyncManifest, SyncStateUnit};
pub use transport::SyncTransport;
