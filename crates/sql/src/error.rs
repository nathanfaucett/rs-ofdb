pub use query::{QueryError as Error, QueryErrorKind as ErrorKind};

#[cfg(any(feature = "redb", feature = "in-memory", feature = "sync"))]
pub(crate) fn from_engine(error: engine::EngineError) -> Error {
    use engine::EngineError;
    use query::QueryErrorKind;

    let kind = match &error {
        EngineError::TranslateError(_) => QueryErrorKind::Validation,
        EngineError::InvalidQuery(_) => QueryErrorKind::Rejected,
        EngineError::Unsupported(_) | EngineError::SyncDependencyUnavailable => {
            QueryErrorKind::Unsupported
        }
        EngineError::Custom(_) => QueryErrorKind::Storage,
    };
    Error::new(kind, error.to_string())
}
