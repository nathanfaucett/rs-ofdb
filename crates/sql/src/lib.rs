#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod api;
#[cfg(any(feature = "remote", feature = "redb", feature = "in-memory"))]
mod client;
#[cfg(any(feature = "redb", feature = "in-memory"))]
mod database;
mod error;

pub use api::*;
#[cfg(any(feature = "remote", feature = "redb", feature = "in-memory"))]
pub use client::{Client, Transaction};
#[cfg(any(feature = "redb", feature = "in-memory"))]
pub use database::Database;
pub use error::{Error, ErrorKind};
