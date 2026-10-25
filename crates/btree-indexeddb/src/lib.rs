#![forbid(unsafe_code)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod database;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod platform;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod tree;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use database::{Database, Snapshot, Table};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use tree::{ByteBTree, Transaction};
