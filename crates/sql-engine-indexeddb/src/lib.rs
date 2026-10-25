#![forbid(unsafe_code)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod kernel;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use kernel::{IndexedDbKernel, IndexedDbTransaction};
