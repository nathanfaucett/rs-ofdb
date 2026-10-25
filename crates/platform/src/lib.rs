#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(feature = "std")]
mod clock;
mod execution;
#[cfg(feature = "time")]
mod time;

#[cfg(feature = "std")]
pub use clock::unix_millis;
pub use execution::{BoxFuture, MaybeSend, MaybeSync};
#[cfg(feature = "time")]
pub use time::{sleep, timeout};
