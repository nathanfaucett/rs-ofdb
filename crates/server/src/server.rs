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
}

impl<E> Server<E> {
    pub fn tcp(address: SocketAddr, executor: E) -> Self {
        Self {
            endpoint: Endpoint::Tcp(address),
            service: QueryService::new(executor),
        }
    }

    #[cfg(unix)]
    pub fn unix(path: impl Into<PathBuf>, executor: E) -> Self {
        Self {
            endpoint: Endpoint::Unix(path.into()),
            service: QueryService::new(executor),
        }
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
        match self.endpoint {
            Endpoint::Tcp(address) => TonicServer::builder()
                .add_service(service)
                .serve_with_shutdown(address, shutdown)
                .await
                .map_err(Into::into),
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let listener = tokio::net::UnixListener::bind(path)?;
                let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
                TonicServer::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}
