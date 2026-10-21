#![no_std]

extern crate alloc;
#[cfg(any(feature = "iroh", feature = "native-stage"))]
extern crate std;

mod candidate;
mod codec;
mod direction;
#[cfg(feature = "native-stage")]
mod file_stage;
#[cfg(feature = "native-stage")]
pub use file_stage::{FileSyncStage, FileSyncStageFactory};
#[cfg(feature = "iroh")]
mod iroh_transport;
mod pre_apply;
mod protocol;
mod scope;
mod session;
mod staging;
mod state;
mod transport;

#[cfg(feature = "continuous")]
pub use sync_runtime::ChangeSubscription;

pub use candidate::{SyncCandidateView, SyncRowView, SyncTableView, SyncValidationDirection};
pub use codec::{InspectedHistory, SyncRowCodec};
pub use direction::SyncDirection;
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
    synchronize_tables_with_stage_factory_candidate_validated_checked,
    synchronize_tables_with_stage_factory_checked,
    synchronize_tables_with_stage_factory_validated_checked, synchronize_with_stage_factory,
    synchronize_with_stage_factory_checked,
};
pub use staging::{MemorySyncStageFactory, SyncStage, SyncStageFactory};
pub use state::{StateDigest, SyncChangeId, SyncKey, SyncManifest, SyncStateUnit};
pub use transport::SyncTransport;
