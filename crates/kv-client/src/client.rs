use proto_kv::kvdb::{
    DeleteRequest, GetRequest, ScanAllRequest, ScanPrefixRequest, ScanRequest, SetRequest,
    kv_service_client::KvServiceClient,
};
use tonic::{transport::Channel, transport::Endpoint};

#[derive(Clone, Debug)]
enum Connector {
    Tcp(Box<Endpoint>),
    #[cfg(all(unix, feature = "unix"))]
    Unix(std::path::PathBuf),
}

#[derive(Clone, Debug)]
pub struct Client {
    connector: Connector,
}

impl Client {
    pub fn lazy_tcp(uri: impl AsRef<str>) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(uri.as_ref().to_string())?;
        Ok(Self {
            connector: Connector::Tcp(Box::new(endpoint)),
        })
    }

    #[cfg(all(unix, feature = "unix"))]
    pub fn lazy_unix(path: impl Into<std::path::PathBuf>) -> Result<Self, tonic::transport::Error> {
        Ok(Self {
            connector: Connector::Unix(path.into()),
        })
    }

    pub async fn get(&self, key: String) -> Result<Option<Vec<u8>>, tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = GetRequest { key };
        let response = client.get(request).await?;
        let inner = response.into_inner();
        Ok(inner.value)
    }

    pub async fn set(
        &self,
        key: String,
        value: Vec<u8>,
        expires_at: Option<i64>,
    ) -> Result<(), tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = SetRequest {
            key,
            value,
            expires_at,
        };
        client.set(request).await?;
        Ok(())
    }

    pub async fn delete(&self, key: String) -> Result<(), tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = DeleteRequest { key };
        client.delete(request).await?;
        Ok(())
    }

    pub async fn scan(
        &self,
        start: String,
        end: String,
    ) -> Result<Vec<(String, Vec<u8>)>, tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = ScanRequest { start, end };
        let response = client.scan(request).await?;
        let inner = response.into_inner();
        let entries = inner
            .entries
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        Ok(entries)
    }

    pub async fn scan_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<(String, Vec<u8>)>, tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = ScanPrefixRequest { prefix };
        let response = client.scan_prefix(request).await?;
        Ok(response
            .into_inner()
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect())
    }

    pub async fn scan_all(&self) -> Result<Vec<(String, Vec<u8>)>, tonic::Status> {
        let mut client = KvServiceClient::new(self.channel());
        let request = ScanAllRequest {};
        let response = client.scan_all(request).await?;
        let inner = response.into_inner();
        let entries = inner
            .entries
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        Ok(entries)
    }

    fn channel(&self) -> Channel {
        match &self.connector {
            Connector::Tcp(endpoint) => endpoint.connect_lazy(),
            #[cfg(all(unix, feature = "unix"))]
            Connector::Unix(path) => {
                use tower::service_fn;
                let path = path.clone();
                // Tonic needs a URI, but this connector ignores it and opens the Unix socket below.
                Endpoint::from_static("http://[::]:50051").connect_with_connector_lazy(service_fn(
                    move |_| {
                        let path = path.clone();
                        async move {
                            tokio::net::UnixStream::connect(path)
                                .await
                                .map(hyper_util::rt::TokioIo::new)
                        }
                    },
                ))
            }
        }
    }
}
