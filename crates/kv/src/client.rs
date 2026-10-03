use core::future::Future;
use std::{path::Path, time::Duration};

use proto_kv::kvdb::{
    DeleteRequest, GetRequest, ScanAllRequest, ScanPrefixRequest, ScanRequest, SetRequest,
    kv_service_client::KvServiceClient,
};
use tonic::transport::{Channel, Endpoint};

use crate::Error;

/// A query-only remote KV client.
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
/// ```
///
/// The KV facade does not export SQL handles:
/// ```compile_fail
/// use ofdb_kv::SqlDatabase;
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    channel: Channel,
    request_deadline: Option<Duration>,
}

impl Client {
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.request_deadline = Some(deadline);
        self
    }

    pub async fn connect(uri: impl AsRef<str>) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(uri.as_ref().to_owned())
            .map_err(|error| Error::Connect(error.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Error::Connect(error.to_string()))?;
        Ok(Self {
            channel,
            request_deadline: None,
        })
    }

    #[cfg(unix)]
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
            channel,
            request_deadline: None,
        })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let response = self
            .request(KvServiceClient::new(self.channel.clone()).get(GetRequest {
                key: key.to_owned(),
            }))
            .await?;
        Ok(response.into_inner().value)
    }

    pub async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        expires_at: Option<i64>,
    ) -> Result<(), Error> {
        self.request(KvServiceClient::new(self.channel.clone()).set(SetRequest {
            key: key.to_owned(),
            value,
            expires_at,
        }))
        .await?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        self.request(
            KvServiceClient::new(self.channel.clone()).delete(DeleteRequest {
                key: key.to_owned(),
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn scan(&self, start: &str, end: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let response = self
            .request(
                KvServiceClient::new(self.channel.clone()).scan(ScanRequest {
                    start: start.to_owned(),
                    end: end.to_owned(),
                }),
            )
            .await?;
        Ok(response
            .into_inner()
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect())
    }

    pub async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let response = self
            .request(
                KvServiceClient::new(self.channel.clone()).scan_prefix(ScanPrefixRequest {
                    prefix: prefix.to_owned(),
                }),
            )
            .await?;
        Ok(response
            .into_inner()
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect())
    }

    pub async fn scan_all(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let response = self
            .request(KvServiceClient::new(self.channel.clone()).scan_all(ScanAllRequest {}))
            .await?;
        Ok(response
            .into_inner()
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect())
    }

    async fn request<T>(
        &self,
        request: impl Future<Output = Result<T, tonic::Status>>,
    ) -> Result<T, Error> {
        let response = match self.request_deadline {
            Some(deadline) => tokio::time::timeout(deadline, request)
                .await
                .map_err(|_| Error::Timeout)?,
            None => request.await,
        };
        response.map_err(map_status)
    }
}

pub(crate) fn map_status(status: tonic::Status) -> Error {
    if let Some((kind, message)) = proto_kv::decode_error_details(status.details()) {
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
