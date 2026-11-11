#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[allow(unsafe_code)]
mod database;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod platform;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod tree;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use database::{Database, Snapshot, Table};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use tree::{ByteBTree, Transaction};
