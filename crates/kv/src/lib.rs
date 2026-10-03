#![forbid(unsafe_code)]

mod error;

#[cfg(feature = "remote")]
mod client;
#[cfg(any(feature = "in-memory", feature = "redb"))]
mod database;

#[cfg(feature = "remote")]
pub use client::Client;
#[cfg(any(feature = "in-memory", feature = "redb"))]
pub use database::Database;
pub use error::{Error, ErrorKind};

#[cfg(all(test, feature = "remote"))]
mod error_tests;
#[cfg(feature = "server")]
pub use kv_server::Server;
