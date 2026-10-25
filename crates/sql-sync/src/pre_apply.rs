use alloc::string::String;

use engine::EngineError;
use thiserror::Error;

pub(crate) type CheckFuture<'a> = platform::BoxFuture<'a, Result<(), String>>;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type Check<'a> = dyn FnMut() -> CheckFuture<'a> + Send + 'a;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) type Check<'a> = dyn FnMut() -> CheckFuture<'a> + 'a;

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
