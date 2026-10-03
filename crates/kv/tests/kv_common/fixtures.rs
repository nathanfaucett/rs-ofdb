use std::time::Duration;

use btree::BTree;
use kv_client::Client;
use kv_server::Server;
use kv_store::KvStore;
use tokio::sync::oneshot;

fn test_timestamp_provider() -> uuid::Timestamp {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    let millis = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    uuid::Timestamp::from_unix_time(
        1_700_000_000 + millis / 1_000,
        (millis % 1_000) as u32 * 1_000_000,
        0,
        0,
    )
}

pub type Shutdown = oneshot::Sender<()>;
pub type ServerTask = tokio::task::JoinHandle<()>;

pub async fn stop_server(mut server: ServerTask) {
    match tokio::time::timeout(Duration::from_secs(3), &mut server).await {
        Ok(result) => result.expect("KV server task should complete"),
        Err(_) => {
            server.abort();
            let _ = server.await;
            panic!("KV server task did not stop before the timeout");
        }
    }
}

pub async fn start_client_server<B>(backend: B) -> (Client, Shutdown, ServerTask)
where
    B: BTree<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind KV server listener");
    let address = listener.local_addr().expect("read KV server address");
    let (shutdown, signal) = oneshot::channel();
    let server = tokio::spawn(async move {
        Server::tcp_listener(listener, KvStore::new(backend, test_timestamp_provider))
            .serve(async {
                let _ = signal.await;
            })
            .await
            .expect("KV server should stop cleanly");
    });
    let client = Client::lazy_tcp(format!("http://{address}")).expect("valid KV endpoint");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.get("__readiness__".into()).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("KV server should become ready");
    (client, shutdown, server)
}
