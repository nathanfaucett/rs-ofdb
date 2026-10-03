use core::future::Future;
use std::time::Duration;

#[cfg(feature = "remote")]
use kv_proto::kvdb::{
    DeleteRequest, GetRequest, ScanAllRequest, ScanPrefixRequest, ScanRequest, SetRequest,
    kv_service_client::KvServiceClient,
};
#[cfg(feature = "remote")]
use std::path::Path;
#[cfg(feature = "remote")]
use tonic::transport::{Channel, Endpoint};

use crate::Error;

/// A query-only client for embedded or remote KV storage.
///
/// It does not expose embedded transactions:
/// ```compile_fail
/// fn no_transaction(client: &ofdb_kv::Client) {
///     let _ = client.transaction();
/// }
/// ```
///
/// It does not expose sync setup:
/// ```compile_fail
/// fn no_sync(client: &ofdb_kv::Client) {
///     let _ = client.synchronize();
/// }
///
/// ```
/// The KV facade does not export SQL handles:
/// ```compile_fail
/// use ofdb_kv::SqlDatabase;
/// ```
#[derive(Clone)]
pub struct Client {
    backend: Backend,
    request_deadline: Option<Duration>,
}

enum Backend {
    #[cfg(any(feature = "in-memory", feature = "redb"))]
    Embedded(crate::Database),
    #[cfg(feature = "remote")]
    Remote(Channel),
}

impl Clone for Backend {
    fn clone(&self) -> Self {
        match self {
            #[cfg(any(feature = "in-memory", feature = "redb"))]
            Self::Embedded(database) => Self::Embedded(database.clone_shared()),
            #[cfg(feature = "remote")]
            Self::Remote(channel) => Self::Remote(channel.clone()),
        }
    }
}

impl core::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Client")
            .field(
                "backend",
                &match &self.backend {
                    #[cfg(any(feature = "in-memory", feature = "redb"))]
                    Backend::Embedded(_) => "Embedded",
                    #[cfg(feature = "remote")]
                    Backend::Remote(_) => "Remote",
                },
            )
            .field("request_deadline", &self.request_deadline)
            .finish()
    }
}

impl Client {
    #[cfg(any(feature = "in-memory", feature = "redb"))]
    pub(crate) fn embedded(database: crate::Database) -> Self {
        Self {
            backend: Backend::Embedded(database),
            request_deadline: None,
        }
    }

    /// Sets an optional deadline for each query. Timed embedded queries need a Tokio runtime
    /// with time enabled. Untimed queries do not need a Tokio runtime.
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.request_deadline = Some(deadline);
        self
    }

    #[cfg(feature = "remote")]
    pub async fn connect(uri: impl AsRef<str>) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(uri.as_ref().to_owned())
            .map_err(|error| Error::Connect(error.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Error::Connect(error.to_string()))?;
        Ok(Self {
            backend: Backend::Remote(channel),
            request_deadline: None,
        })
    }

    #[cfg(all(feature = "remote", unix))]
    pub async fn connect_unix(path: impl AsRef<Path>) -> Result<Self, Error> {
        use hyper_util::rt::TokioIo;
        use tower::service_fn;

        let path = path.as_ref().to_owned();
        let channel = Endpoint::from_static("http://[::]:50051")
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map(TokioIo::new)
                }
            }))
            .await
            .map_err(|error| Error::Connect(error.to_string()))?;
        Ok(Self {
            backend: Backend::Remote(channel),
            request_deadline: None,
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_get(key).await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    let response = KvServiceClient::new(channel.clone())
                        .get(GetRequest {
                            key: key.to_owned(),
                        })
                        .await
                        .map_err(map_status)?;
                    Ok(response.into_inner().value)
                }
            }
        })
        .await
    }

    pub async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        expires_at: Option<i64>,
    ) -> Result<(), Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_set(key, value, expires_at).await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    KvServiceClient::new(channel.clone())
                        .set(SetRequest {
                            key: key.to_owned(),
                            value,
                            expires_at,
                        })
                        .await
                        .map_err(map_status)?;
                    Ok(())
                }
            }
        })
        .await
    }

    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_delete(key).await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    KvServiceClient::new(channel.clone())
                        .delete(DeleteRequest {
                            key: key.to_owned(),
                        })
                        .await
                        .map_err(map_status)?;
                    Ok(())
                }
            }
        })
        .await
    }

    pub async fn scan(&self, start: &str, end: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_scan(start, end).await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    let response = KvServiceClient::new(channel.clone())
                        .scan(ScanRequest {
                            start: start.to_owned(),
                            end: end.to_owned(),
                        })
                        .await
                        .map_err(map_status)?;
                    Ok(response
                        .into_inner()
                        .entries
                        .into_iter()
                        .map(|entry| (entry.key, entry.value))
                        .collect())
                }
            }
        })
        .await
    }

    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_scan_prefix(prefix).await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    let response = KvServiceClient::new(channel.clone())
                        .scan_prefix(ScanPrefixRequest {
                            prefix: prefix.to_owned(),
                        })
                        .await
                        .map_err(map_status)?;
                    Ok(response
                        .into_inner()
                        .entries
                        .into_iter()
                        .map(|entry| (entry.key, entry.value))
                        .collect())
                }
            }
        })
        .await
    }

    pub async fn scan_all(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        self.request(async {
            match &self.backend {
                #[cfg(any(feature = "in-memory", feature = "redb"))]
                Backend::Embedded(database) => database.query_scan_all().await,
                #[cfg(feature = "remote")]
                Backend::Remote(channel) => {
                    let response = KvServiceClient::new(channel.clone())
                        .scan_all(ScanAllRequest {})
                        .await
                        .map_err(map_status)?;
                    Ok(response
                        .into_inner()
                        .entries
                        .into_iter()
                        .map(|entry| (entry.key, entry.value))
                        .collect())
                }
            }
        })
        .await
    }

    async fn request<T>(
        &self,
        request: impl Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        match self.request_deadline {
            Some(deadline) => tokio::time::timeout(deadline, request)
                .await
                .map_err(|_| Error::Timeout)?,
            None => request.await,
        }
    }
}

#[cfg(feature = "remote")]
pub(crate) fn map_status(status: tonic::Status) -> Error {
    if let Some((kind, message)) = kv_proto::decode_error_details(status.details()) {
        return Error::Query {
            kind: kind.into(),
            message: message.to_owned(),
        };
    }
    match status.code() {
        tonic::Code::DeadlineExceeded => Error::Timeout,
        tonic::Code::Unavailable | tonic::Code::Cancelled => {
            Error::Transport(status.message().to_owned())
        }
        _ => Error::Query {
            kind: crate::ErrorKind::Internal,
            message: status.message().to_owned(),
        },
    }
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use core::future::pending;
    use std::time::Duration;

    use crate::Error;

    #[tokio::test]
    async fn deadline_wraps_pending_work_and_is_independent_after_clone() {
        let database = crate::Database::in_memory();
        let client = database.client();
        let timed = client.clone().with_deadline(Duration::ZERO);
        assert!(matches!(
            timed.request::<()>(pending::<Result<(), Error>>()).await,
            Err(Error::Timeout)
        ));
        assert_eq!(client.request_deadline, None);
    }
}
