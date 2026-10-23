#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod btree;
#[cfg(feature = "changes")]
mod changes;
#[cfg(all(test, feature = "changes"))]
mod changes_tests;
#[cfg(feature = "changes")]
pub use changes::{ChangeSignal, ChangeSubscription, ChangeWork, StorageInvalidation};
#[cfg(feature = "in-memory")]
mod in_memory_btree;
#[cfg(feature = "sharded")]
mod sharded_btree;

pub use btree::{
    BTree, BTreeError, BTreeKey, BTreeRead, BTreeResult, BTreeTransaction, BTreeValue,
};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::InMemoryBTree;
#[cfg(all(feature = "sharded", feature = "in-memory"))]
pub use in_memory_btree::InMemoryBTreeShardFactory;
#[cfg(feature = "sharded")]
pub use sharded_btree::{ShardedBTree, ShardedBTreeTransaction, Sharder};
