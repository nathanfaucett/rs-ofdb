use core::future::Future;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use btree::{BTree, BTreeError};
use kv::KvStore;
use kv_proto as proto;
use kv_proto::kvdb::{
    DeleteResponse, GetResponse, ScanAllResponse, ScanEntry, ScanResponse, SetResponse,
};
use tonic::{Request, Response, Status};

#[derive(Debug)]
enum Endpoint {
    Tcp(SocketAddr),
    TcpListener(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Clone)]
pub struct KvService<S>
where
    S: BTree<Vec<u8>, Vec<u8>>,
{
    store: Arc<KvStore<S>>,
    max_decoding_message_size: Option<usize>,
}

impl<S> KvService<S>
where
    S: BTree<Vec<u8>, Vec<u8>>,
{
    pub fn new(store: KvStore<S>) -> Self {
        Self {
            store: Arc::new(store),
            max_decoding_message_size: None,
        }
    }

    pub fn max_decoding_message_size(mut self, size: usize) -> Self {
        self.max_decoding_message_size = Some(size);
        self
    }

    pub fn into_tonic_service(self) -> proto::kvdb::kv_service_server::KvServiceServer<Self> {
        let max_decoding_message_size = self.max_decoding_message_size;
        let service = proto::kvdb::kv_service_server::KvServiceServer::new(self);
        match max_decoding_message_size {
            Some(size) => service.max_decoding_message_size(size),
            None => service,
        }
    }
}

#[tonic::async_trait]
impl<S> proto::kvdb::kv_service_server::KvService for KvService<S>
where
    S: BTree<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
{
    type TransactionStream = crate::transaction::TransactionStream;

    async fn transaction(
        &self,
        request: Request<tonic::Streaming<proto::kvdb::TransactionRequest>>,
    ) -> Result<Response<Self::TransactionStream>, Status> {
        let transaction = self.store.transaction().await.map_err(status_from_error)?;
        Ok(Response::new(crate::transaction::stream(
            transaction,
            request.into_inner(),
        )))
    }

    async fn get(
        &self,
        request: Request<proto::kvdb::GetRequest>,
    ) -> Result<Response<proto::kvdb::GetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let now = current_time_millis()?;

        let tx = self.store.transaction().await.map_err(status_from_error)?;
        let value = tx.get(&key, now).await.map_err(status_from_error)?;

        Ok(Response::new(GetResponse {
            value: value.map(kv_proto::value_to_proto),
        }))
    }

    async fn set(
        &self,
        request: Request<proto::kvdb::SetRequest>,
    ) -> Result<Response<proto::kvdb::SetResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = kv_proto::required_value(request.value)?;
        let expires_at = request.expires_at;

        let mut tx = self.store.transaction().await.map_err(status_from_error)?;
        tx.set(&key, value, expires_at)
            .await
            .map_err(status_from_error)?;
        tx.commit().await.map_err(status_from_error)?;

        Ok(Response::new(SetResponse {}))
    }

    async fn delete(
        &self,
        request: Request<proto::kvdb::DeleteRequest>,
    ) -> Result<Response<proto::kvdb::DeleteResponse>, Status> {
        let request = request.into_inner();
        let key = request.key;

        let mut tx = self.store.transaction().await.map_err(status_from_error)?;
        tx.delete(&key).await.map_err(status_from_error)?;
        tx.commit().await.map_err(status_from_error)?;

        Ok(Response::new(DeleteResponse {}))
    }

    async fn scan(
        &self,
        request: Request<proto::kvdb::ScanRequest>,
    ) -> Result<Response<proto::kvdb::ScanResponse>, Status> {
        let request = request.into_inner();
        let start = request.start;
        let end = request.end;
        let now = current_time_millis()?;

        let tx = self.store.transaction().await.map_err(status_from_error)?;
        let entries = tx.scan(start..end, now).await.map_err(status_from_error)?;

        let entries = entries
            .into_iter()
            .map(|(key, value)| ScanEntry {
                key,
                value: Some(kv_proto::value_to_proto(value)),
            })
            .collect();

        Ok(Response::new(ScanResponse { entries }))
    }

    async fn scan_prefix(
        &self,
        request: Request<proto::kvdb::ScanPrefixRequest>,
    ) -> Result<Response<proto::kvdb::ScanResponse>, Status> {
        let request = request.into_inner();
        let now = current_time_millis()?;
        let entries = self
            .store
            .transaction()
            .await
            .map_err(status_from_error)?
            .scan_prefix(&request.prefix, now)
            .await
            .map_err(status_from_error)?;

        let entries = entries
            .into_iter()
            .map(|(key, value)| ScanEntry {
                key,
                value: Some(kv_proto::value_to_proto(value)),
            })
            .collect();

        Ok(Response::new(ScanResponse { entries }))
    }

    async fn scan_all(
        &self,
        request: Request<proto::kvdb::ScanAllRequest>,
    ) -> Result<Response<proto::kvdb::ScanAllResponse>, Status> {
        let _request = request.into_inner();
        let now = current_time_millis()?;

        let tx = self.store.transaction().await.map_err(status_from_error)?;
        let entries = tx.scan_all(now).await.map_err(status_from_error)?;

        let entries = entries
            .into_iter()
            .map(|(key, value)| ScanEntry {
                key,
                value: Some(kv_proto::value_to_proto(value)),
            })
            .collect();

        Ok(Response::new(ScanAllResponse { entries }))
    }
}

pub struct Server<S>
where
    S: BTree<Vec<u8>, Vec<u8>>,
{
    endpoint: Endpoint,
    service: KvService<S>,
    tls_identity: Option<tonic::transport::Identity>,
    tls_client_ca: Option<tonic::transport::Certificate>,
}

impl<S> Server<S>
where
    S: BTree<Vec<u8>, Vec<u8>>,
{
    pub fn tcp(address: SocketAddr, store: KvStore<S>) -> Self {
        Self {
            endpoint: Endpoint::Tcp(address),
            service: KvService::new(store),
            tls_identity: None,
            tls_client_ca: None,
        }
    }

    pub fn tcp_listener(listener: tokio::net::TcpListener, store: KvStore<S>) -> Self {
        Self {
            endpoint: Endpoint::TcpListener(listener),
            service: KvService::new(store),
            tls_identity: None,
            tls_client_ca: None,
        }
    }

    #[cfg(unix)]
    pub fn unix(path: impl Into<PathBuf>, store: KvStore<S>) -> Self {
        Self {
            endpoint: Endpoint::Unix(path.into()),
            service: KvService::new(store),
            tls_identity: None,
            tls_client_ca: None,
        }
    }

    pub fn tls_identity(mut self, certificate: Vec<u8>, private_key: Vec<u8>) -> Self {
        self.tls_identity = Some(tonic::transport::Identity::from_pem(
            certificate,
            private_key,
        ));
        self
    }

    pub fn tls_client_ca(mut self, certificate: Vec<u8>) -> Self {
        self.tls_client_ca = Some(tonic::transport::Certificate::from_pem(certificate));
        self
    }

    pub fn max_decoding_message_size(mut self, size: usize) -> Self {
        self.service = self.service.max_decoding_message_size(size);
        self
    }

    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        S: BTree<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
    {
        let service = self.service.into_tonic_service();
        let mut builder = tonic::transport::Server::builder();
        if let Some(identity) = self.tls_identity {
            let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            if let Some(client_ca) = self.tls_client_ca {
                tls = tls.client_ca_root(client_ca);
            }
            builder = builder.tls_config(tls)?;
        }
        match self.endpoint {
            Endpoint::Tcp(address) => builder
                .add_service(service)
                .serve_with_shutdown(address, shutdown)
                .await
                .map_err(Into::into),
            Endpoint::TcpListener(listener) => {
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                builder
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
                    .map_err(Into::into)
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let listener = tokio::net::UnixListener::bind(path)?;
                let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
                builder
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}

pub(crate) fn current_time_millis() -> Result<i64, Status> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(status_from_error)?
        .as_millis();
    i64::try_from(millis).map_err(status_from_error)
}

pub(crate) fn status_from_error(error: impl std::error::Error + 'static) -> Status {
    let kind = if let Some(error) = (&error as &dyn std::error::Error).downcast_ref::<BTreeError>()
    {
        match error {
            BTreeError::InvalidDocument => proto::ErrorKind::InvalidDocument,
            BTreeError::TypeMismatch => proto::ErrorKind::TypeMismatch,
            BTreeError::Conflict => proto::ErrorKind::Conflict,
            BTreeError::CommitFailed => proto::ErrorKind::CommitFailed,
            BTreeError::RollbackFailed => proto::ErrorKind::RollbackFailed,
            BTreeError::UnsupportedOperation => proto::ErrorKind::UnsupportedOperation,
            BTreeError::Custom(_) => proto::ErrorKind::Storage,
        }
    } else {
        proto::ErrorKind::Internal
    };
    let message = error.to_string();
    let details = proto::encode_error_details(kind, &message);
    Status::with_details(
        tonic::Code::Internal,
        message,
        tonic::codegen::Bytes::from(details),
    )
}
