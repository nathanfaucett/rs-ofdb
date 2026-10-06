use core::future::Future;
use std::{error::Error, net::SocketAddr, path::PathBuf};

use protocol::QueryExecutor;
use tonic::transport::Server as TonicServer;

use crate::QueryService;

#[derive(Debug)]
enum Endpoint {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Debug)]
pub struct Server<E> {
    endpoint: Endpoint,
    service: QueryService<E>,
    tls_identity: Option<tonic::transport::Identity>,
    tls_client_ca: Option<tonic::transport::Certificate>,
}

impl<E> Server<E> {
    pub fn tcp(address: SocketAddr, executor: E) -> Self {
        Self {
            endpoint: Endpoint::Tcp(address),
            service: QueryService::new(executor),
            tls_identity: None,
            tls_client_ca: None,
        }
    }

    #[cfg(unix)]
    pub fn unix(path: impl Into<PathBuf>, executor: E) -> Self {
        Self {
            endpoint: Endpoint::Unix(path.into()),
            service: QueryService::new(executor),
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

    pub fn tls_client_ca(mut self, ca_certificate: Vec<u8>) -> Self {
        self.tls_client_ca = Some(tonic::transport::Certificate::from_pem(ca_certificate));
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
        E: QueryExecutor + 'static,
    {
        let service = self.service.into_tonic_service();
        let mut builder = TonicServer::builder();
        if self.tls_identity.is_some() || self.tls_client_ca.is_some() {
            let mut tls = tonic::transport::ServerTlsConfig::new();
            if let Some(identity) = self.tls_identity {
                tls = tls.identity(identity);
            }
            if let Some(ca) = self.tls_client_ca {
                tls = tls.client_ca_root(ca);
            }
            builder = builder.tls_config(tls)?;
        }
        match self.endpoint {
            Endpoint::Tcp(address) => builder
                .add_service(service)
                .serve_with_shutdown(address, shutdown)
                .await
                .map_err(Into::into),
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
