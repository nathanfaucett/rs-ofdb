use futures::executor::block_on;
use ofdb_kv::Database;

#[test]
fn embedded_client_owns_shared_storage_without_tokio() {
    block_on(async {
        let database = Database::in_memory();
        let client = database.client();
        let shared = database.client();
        let clone = client.clone();

        client
            .set("bytes", vec![0, 255, 1], None)
            .await
            .expect("write opaque bytes");
        assert_eq!(
            shared
                .get("bytes")
                .await
                .expect("database sees client write"),
            Some(vec![0, 255, 1])
        );
        assert_eq!(
            clone.get("bytes").await.expect("clone sees client write"),
            Some(vec![0, 255, 1])
        );
        shared
            .set("other", vec![], None)
            .await
            .expect("database write");
        assert_eq!(
            client.scan_all().await.expect("ordered scan"),
            vec![("bytes".into(), vec![0, 255, 1]), ("other".into(), vec![])]
        );
        assert!(
            client
                .scan_prefix("by")
                .await
                .expect("literal prefix")
                .len()
                == 1
        );
        assert_eq!(
            client
                .scan("bytes", "other")
                .await
                .expect("exclusive upper bound")
                .len(),
            1
        );
        client.delete("missing").await.expect("delete missing key");
        assert_eq!(
            client.get("missing").await.expect("missing get succeeds"),
            None
        );
        client
            .set("expired", vec![1], Some(0))
            .await
            .expect("set past expiry");
        assert_eq!(
            client.get("expired").await.expect("expired get succeeds"),
            None
        );
    });
}

#[test]
fn client_remains_usable_after_database_drop() {
    let client = {
        let database = Database::in_memory();
        database.client()
    };
    block_on(async {
        client
            .set("alive", vec![7], None)
            .await
            .expect("write after database drop");
        assert_eq!(
            client.get("alive").await.expect("read after database drop"),
            Some(vec![7])
        );
    });
}
