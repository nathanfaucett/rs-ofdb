#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod error;
mod query;
mod result;
mod translator;

pub use error::{QueryError, QueryErrorKind};
pub use query::*;
pub use translator::*;
