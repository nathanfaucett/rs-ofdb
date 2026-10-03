#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod api;
#[cfg(feature = "remote")]
mod client;
#[cfg(any(feature = "redb", feature = "in-memory"))]
mod database;
mod error;

pub use api::*;
#[cfg(feature = "remote")]
pub use client::Client;
#[cfg(any(feature = "redb", feature = "in-memory"))]
pub use database::{Database, DatabaseTransaction};
pub use error::{Error, ErrorKind};
