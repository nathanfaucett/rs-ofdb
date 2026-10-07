use alloc::{boxed::Box, string::String};
use core::{future::Future, pin::Pin};

use engine::EngineError;
use thiserror::Error;

pub(crate) type CheckFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
pub(crate) type Check<'a> = dyn FnMut() -> CheckFuture<'a> + Send + 'a;

#[derive(Debug, Error)]
pub(crate) enum ApplyError {
    #[error("{0}")]
    Engine(#[from] EngineError),
    #[error("pre-apply check failed: {0}")]
    PreApply(String),
}
impl ApplyError {
    pub(crate) fn into_engine(self) -> EngineError {
        match self {
            Self::Engine(error) => error,
            Self::PreApply(reason) => EngineError::custom(reason),
        }
    }
}
pub(crate) async fn check_apply(check: &mut Check<'_>) -> Result<(), ApplyError> {
    check().await.map_err(ApplyError::PreApply)
}

impl<E> From<ApplyError> for crate::SyncError<E> {
    fn from(error: ApplyError) -> Self {
        match error {
            ApplyError::Engine(error) => Self::Engine(error),
            ApplyError::PreApply(reason) => Self::PreApply(reason),
        }
    }
}
