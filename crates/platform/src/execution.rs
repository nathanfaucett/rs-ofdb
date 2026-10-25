use alloc::boxed::Box;
use core::{future::Future, pin::Pin};

/// Native work remains transferable between threads; browser work stays local.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use core::marker::{Send as MaybeSend, Sync as MaybeSync};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait MaybeSend {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> MaybeSend for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait MaybeSync {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> MaybeSync for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
