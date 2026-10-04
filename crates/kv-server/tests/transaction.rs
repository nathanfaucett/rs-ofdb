use std::time::Duration;

use btree::{BTree, BTreeTransaction, InMemoryBTree};
use kv::KvStore;
use kv_proto::{
    ErrorKind, decode_error_details,
    kvdb::{
        DeleteRequest, GetRequest, ScanAllRequest, ScanEntry, ScanPrefixRequest, ScanRequest,
        SetRequest, TransactionRequest, TransactionResponse, kv_service_client::KvServiceClient,
        transaction_request::Command, transaction_response::Outcome,
    },
};
use ofdb_kv_server::Server;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Streaming, transport::Channel};
use value::Value;

struct Host {
    store: KvStore<InMemoryBTree<Vec<u8>, Vec<u8>>>,
    client: KvServiceClient<Channel>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Host {
    async fn new(backend: InMemoryBTree<Vec<u8>, Vec<u8>>) -> Self {
        let store = KvStore::new(backend, || uuid::Timestamp::now(uuid::NoContext));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener must bind");
        let address = listener
            .local_addr()
            .expect("listener must have an address");
        let (shutdown, signal) = oneshot::channel();
        let server = Server::tcp_listener(listener, store.clone());
        let task = tokio::spawn(async move {
            server
                .serve(async {
                    let _ = signal.await;
                })
                .await
                .expect("test server must serve");
        });
        let channel = Channel::from_shared(format!("http://{address}"))
            .expect("test endpoint must be valid")
            .connect()
            .await
            .expect("client must connect");
        let client = KvServiceClient::new(channel);
        Self {
            store,
            client,
            shutdown,
            task,
        }
    }

    async fn transaction(
        &mut self,
    ) -> (
        mpsc::Sender<TransactionRequest>,
        Streaming<TransactionResponse>,
    ) {
        let (sender, receiver) = mpsc::channel(1);
        let mut responses = self
            .client
            .transaction(ReceiverStream::new(receiver))
            .await
            .expect("transaction must start")
            .into_inner();
        assert_eq!(next(&mut responses).await, Some(Outcome::Ready(())));
        (sender, responses)
    }

    async fn stop(self) {
        self.shutdown.send(()).expect("server must accept shutdown");
        timeout(Duration::from_secs(5), self.task)
            .await
            .expect("server must stop")
            .expect("server task must complete");
    }
}

async fn next(responses: &mut Streaming<TransactionResponse>) -> Option<Outcome> {
    timeout(Duration::from_secs(5), responses.message())
        .await
        .expect("response must arrive")
        .expect("stream must not fail")
        .and_then(|response| response.outcome)
}

async fn command(
    sender: &mpsc::Sender<TransactionRequest>,
    responses: &mut Streaming<TransactionResponse>,
    command: Command,
) -> Outcome {
    sender
        .send(TransactionRequest {
            command: Some(command),
        })
        .await
        .expect("stream must accept command");
    next(responses).await.expect("command must have an outcome")
}

fn set(key: &str, value: &[u8], expires_at: Option<i64>) -> Command {
    Command::Set(SetRequest {
        key: key.into(),
        value: Some(kv_proto::value_to_proto(Value::Blob(value.to_vec()))),
        expires_at,
    })
}

#[tokio::test]
async fn transaction_reads_writes_scans_and_commits() {
    let mut host = Host::new(InMemoryBTree::new()).await;
    let (sender, mut responses) = host.transaction().await;
    for request in [
        set("a", b"one", None),
        set("b", b"two", None),
        set("expired", b"old", Some(0)),
    ] {
        assert_eq!(
            command(&sender, &mut responses, request).await,
            Outcome::Completed(())
        );
    }
    assert_eq!(
        command(
            &sender,
            &mut responses,
            Command::Get(GetRequest { key: "a".into() })
        )
        .await,
        Outcome::Got(kv_proto::kvdb::GetResponse {
            value: Some(kv_proto::value_to_proto(Value::Blob(b"one".to_vec())))
        })
    );
    assert_eq!(
        command(
            &sender,
            &mut responses,
            Command::Get(GetRequest {
                key: "expired".into()
            })
        )
        .await,
        Outcome::Got(kv_proto::kvdb::GetResponse { value: None })
    );
    for (request, keys) in [
        (
            Command::Scan(ScanRequest {
                start: "a".into(),
                end: "b".into(),
            }),
            vec!["a"],
        ),
        (
            Command::ScanPrefix(ScanPrefixRequest { prefix: "b".into() }),
            vec!["b"],
        ),
        (Command::ScanAll(ScanAllRequest {}), vec!["a", "b"]),
    ] {
        let Outcome::Scanned(response) = command(&sender, &mut responses, request).await else {
            panic!("scan must return entries");
        };
        assert_eq!(
            response
                .entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            keys
        );
        assert!(response.entries.iter().all(|entry| entry.value.is_some()));
    }
    assert_eq!(
        command(
            &sender,
            &mut responses,
            Command::Delete(DeleteRequest { key: "a".into() })
        )
        .await,
        Outcome::Completed(())
    );
    assert_eq!(
        command(&sender, &mut responses, Command::Commit(())).await,
        Outcome::Completed(())
    );
    assert_eq!(next(&mut responses).await, None);
    drop(sender);
    let tx = host
        .store
        .transaction()
        .await
        .expect("verification transaction must start");
    assert_eq!(tx.get("a", 0).await.expect("read must succeed"), None);
    assert_eq!(
        tx.get("b", 0).await.expect("read must succeed"),
        Some(Value::Blob(b"two".to_vec()))
    );
    tx.rollback().await.expect("verification must roll back");
    host.stop().await;
}

#[tokio::test]
async fn rollback_and_stream_closure_discard_writes() {
    let mut host = Host::new(InMemoryBTree::new()).await;
    for mode in 0..3 {
        let (sender, mut responses) = host.transaction().await;
        assert_eq!(
            command(&sender, &mut responses, set("uncommitted", b"value", None)).await,
            Outcome::Completed(())
        );
        match mode {
            0 => {
                assert_eq!(
                    command(&sender, &mut responses, Command::Rollback(())).await,
                    Outcome::Completed(())
                );
                assert_eq!(next(&mut responses).await, None);
            }
            1 => {
                drop(sender);
                assert_eq!(next(&mut responses).await, None);
            }
            _ => {
                drop(responses);
                // Keep the request stream open while the response stream is cancelled.
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(sender);
            }
        }
        let tx = host
            .store
            .transaction()
            .await
            .expect("verification transaction must start");
        assert_eq!(
            tx.get("uncommitted", 0).await.expect("read must succeed"),
            None
        );
        tx.rollback().await.expect("verification must roll back");
    }
    host.stop().await;
}

#[tokio::test]
async fn operation_error_preserves_category_and_transaction_remains_usable() {
    let backend = InMemoryBTree::new();
    let mut tx = backend
        .transaction()
        .await
        .expect("backend transaction must start");
    tx.insert(Vec::new(), Vec::new())
        .await
        .expect("corrupt fixture must be inserted");
    tx.commit().await.expect("fixture must commit");
    let mut host = Host::new(backend).await;
    let (sender, mut responses) = host.transaction().await;
    let Outcome::ErrorDetail(details) =
        command(&sender, &mut responses, Command::ScanAll(ScanAllRequest {})).await
    else {
        panic!("corrupt data must return an error");
    };
    assert_eq!(
        decode_error_details(&details).map(|(kind, _)| kind),
        Some(ErrorKind::InvalidDocument)
    );
    assert_eq!(
        command(&sender, &mut responses, set("valid", b"value", None)).await,
        Outcome::Completed(())
    );
    let Outcome::Scanned(response) = command(
        &sender,
        &mut responses,
        Command::ScanPrefix(ScanPrefixRequest {
            prefix: "valid".into(),
        }),
    )
    .await
    else {
        panic!("transaction must remain usable");
    };
    assert_eq!(
        response.entries,
        vec![ScanEntry {
            key: "valid".into(),
            value: Some(kv_proto::value_to_proto(Value::Blob(b"value".to_vec())))
        }]
    );
    assert_eq!(
        command(&sender, &mut responses, Command::Commit(())).await,
        Outcome::Completed(())
    );
    assert_eq!(next(&mut responses).await, None);
    drop(sender);
    host.stop().await;
}
