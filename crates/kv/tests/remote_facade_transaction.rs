use ofdb_kv::{Client, Database};
use std::sync::Arc;
use tokio::sync::oneshot;

#[tokio::test]
async fn remote_client_transaction_commits_and_rolls_back() {
    let database = Arc::new(Database::in_memory());
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("read test address");
    drop(listener);
    let (shutdown, stopped) = oneshot::channel::<()>();
    let server_database = database.clone();
    let server = tokio::spawn(async move {
        server_database
            .serve_tcp(address, async move {
                let _ = stopped.await;
            })
            .await
            .expect("serve test database");
    });

    let endpoint = format!("http://{address}");
    let mut connected = None;
    for _ in 0..50 {
        match Client::connect(&endpoint).await {
            Ok(client) => {
                connected = Some(client);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    let client = connected.expect("connect remote client");
    let mut transaction = client
        .transaction()
        .await
        .expect("start remote transaction");
    transaction
        .set("committed", vec![1], None)
        .await
        .expect("set committed value");
    assert_eq!(
        transaction
            .get("committed")
            .await
            .expect("read in transaction"),
        Some(vec![1])
    );
    transaction
        .commit()
        .await
        .expect("commit remote transaction");
    assert_eq!(
        client.get("committed").await.expect("read committed value"),
        Some(vec![1])
    );

    let mut transaction = client
        .transaction()
        .await
        .expect("start rollback transaction");
    transaction
        .set("rolled-back", vec![2], None)
        .await
        .expect("set rolled-back value");
    transaction
        .rollback()
        .await
        .expect("rollback remote transaction");
    assert_eq!(
        client
            .get("rolled-back")
            .await
            .expect("read rolled-back key"),
        None
    );

    let _ = shutdown.send(());
    server.await.expect("join server task");
}
