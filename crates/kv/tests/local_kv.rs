use std::sync::Arc;

use btree::{BTree, InMemoryBTree};
use btree_redb::{Bytes, RedbByteBTree, table_definition};
use kv_store::KvStore;
use redb::Database;
use value::Value;

const TABLE: &str = "local-kv-test";

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

async fn exercise_store<B>(backend: B)
where
    B: BTree<Vec<u8>, Vec<u8>> + Clone,
{
    let store = KvStore::new(backend.clone(), test_timestamp_provider);
    let mut tx = store
        .transaction()
        .await
        .expect("start deterministic transaction");

    tx.set("test:1234", Value::Blob(vec![1]), None)
        .await
        .expect("set test key");
    tx.set("testX", Value::Blob(vec![2]), None)
        .await
        .expect("set adjacent key");
    tx.set("prefix|get|5", Value::Blob(vec![3]), None)
        .await
        .expect("set delimited prefix key");
    tx.set("prefixX", Value::Blob(vec![4]), None)
        .await
        .expect("set adjacent prefix key");
    tx.set("taco-1", Value::Blob(vec![5]), None)
        .await
        .expect("set taco key");
    tx.set("雪\0:key", Value::Blob(vec![6]), None)
        .await
        .expect("set Unicode key with NUL");
    tx.set("literal*key", Value::Blob(vec![7]), None)
        .await
        .expect("set literal star key");
    tx.set("empty-value", Value::Blob(vec![99]), None)
        .await
        .expect("set initial value");
    tx.set("empty-value", Value::Blob(Vec::new()), None)
        .await
        .expect("overwrite with empty value");
    tx.set("expires", Value::Blob(vec![8]), Some(10))
        .await
        .expect("set expiring value");
    tx.set("deleted", Value::Blob(vec![9]), None)
        .await
        .expect("set soon-to-be-deleted key");
    tx.delete("deleted").await.expect("delete key");
    tx.delete("deleted")
        .await
        .expect("delete key a second time");
    tx.set("recreated", Value::Blob(vec![10]), None)
        .await
        .expect("set old generation");
    let old = tx
        .export_snapshot("recreated")
        .await
        .expect("export old generation")
        .expect("old generation exists");
    tx.delete("recreated").await.expect("delete old generation");
    tx.set("recreated", Value::Blob(vec![11]), None)
        .await
        .expect("create new generation");
    let new = tx
        .export_snapshot("recreated")
        .await
        .expect("export new generation")
        .expect("new generation exists");
    assert!(
        kv_store::decode_document_id(&new.0.id)
            .expect("decode new ID")
            .1
            > kv_store::decode_document_id(&old.0.id)
                .expect("decode old ID")
                .1
    );
    tx.import_snapshot(old)
        .await
        .expect("ignore stale generation");

    assert_eq!(tx.get("missing", 0).await.expect("get missing"), None);
    assert_eq!(
        tx.get("empty-value", 0).await.expect("get empty"),
        Some(Value::Blob(vec![]))
    );
    assert_eq!(
        tx.get("recreated", 0).await.expect("get recreated"),
        Some(Value::Blob(vec![11]))
    );
    assert_eq!(tx.get("deleted", 0).await.expect("get deleted"), None);
    assert_eq!(
        tx.get("expires", 9).await.expect("get before expiry"),
        Some(Value::Blob(vec![8]))
    );
    assert_eq!(tx.get("expires", 10).await.expect("get at expiry"), None);

    assert_eq!(
        tx.scan("prefix".into().."prefix~".into(), 0)
            .await
            .expect("scan range"),
        vec![
            ("prefixX".into(), Value::Blob(vec![4])),
            ("prefix|get|5".into(), Value::Blob(vec![3])),
        ]
    );
    assert!(
        tx.scan("test".into().."test".into(), 0)
            .await
            .expect("empty range")
            .is_empty()
    );
    assert!(
        tx.scan("z".into().."a".into(), 0)
            .await
            .expect("reversed range")
            .is_empty()
    );
    assert_eq!(
        tx.scan_prefix("test:", 0).await.expect("scan test prefix"),
        vec![("test:1234".into(), Value::Blob(vec![1]))]
    );
    assert_eq!(
        tx.scan_prefix("prefix|get|", 0)
            .await
            .expect("scan delimited prefix"),
        vec![("prefix|get|5".into(), Value::Blob(vec![3]))]
    );
    assert_eq!(
        tx.scan_prefix("taco-", 0).await.expect("scan taco prefix"),
        vec![("taco-1".into(), Value::Blob(vec![5]))]
    );
    assert_eq!(
        tx.scan_prefix("雪\0:", 0)
            .await
            .expect("scan Unicode prefix"),
        vec![("雪\0:key".into(), Value::Blob(vec![6]))]
    );
    assert_eq!(
        tx.scan_prefix("literal*", 0)
            .await
            .expect("scan literal star prefix"),
        vec![("literal*key".into(), Value::Blob(vec![7]))]
    );
    assert_eq!(
        tx.scan_prefix("", 0).await.expect("scan empty prefix"),
        tx.scan_all(0).await.expect("scan all")
    );
    assert!(
        tx.scan_prefix("not-found", 0)
            .await
            .expect("scan absent prefix")
            .is_empty()
    );

    tx.rollback().await.expect("roll back transaction");
    let mut tx = KvStore::new(backend.clone(), test_timestamp_provider)
        .transaction()
        .await
        .expect("start rollback check");
    assert_eq!(
        tx.get("test:1234", 0).await.expect("read rolled back key"),
        None
    );
    tx.set("committed", Value::Blob(vec![12]), None)
        .await
        .expect("set committed value");
    assert_eq!(
        tx.get("committed", 0).await.expect("read pending value"),
        Some(Value::Blob(vec![12]))
    );
    tx.commit().await.expect("commit value");

    let mut tx = KvStore::new(backend.clone(), test_timestamp_provider)
        .transaction()
        .await
        .expect("start commit verification");
    assert_eq!(
        tx.get("committed", 0).await.expect("read committed value"),
        Some(Value::Blob(vec![12]))
    );
    tx.set("rolled-back", Value::Blob(vec![13]), None)
        .await
        .expect("set value to roll back");
    tx.rollback().await.expect("roll back value");

    let tx = KvStore::new(backend, test_timestamp_provider)
        .transaction()
        .await
        .expect("start rollback verification");
    assert_eq!(
        tx.get("rolled-back", 0)
            .await
            .expect("read rolled-back value"),
        None
    );
    tx.rollback().await.expect("finish rollback verification");
}

#[tokio::test]
async fn in_memory_store_supports_kv_operations_and_scans() {
    exercise_store(InMemoryBTree::<Vec<u8>, Vec<u8>>::new()).await;
}

#[tokio::test]
async fn redb_store_supports_kv_operations_and_scans() {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let path = directory.path().join("kv.redb");
    let database = Arc::new(Database::create(&path).expect("create Redb database"));
    let write = database.begin_write().expect("start table transaction");
    write
        .open_table(table_definition::<Bytes, Vec<u8>>(TABLE))
        .expect("create KV table");
    write.commit().expect("commit KV table");

    exercise_store(RedbByteBTree::new(database.clone(), TABLE)).await;
}
