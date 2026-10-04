use std::sync::Arc;

use btree_redb::{Bytes, RedbByteBTree, table_definition};
use futures::executor::block_on;
use ofdb_kv_store::KvStore;
use redb::Database;
use uuid::Uuid;
use value::Value;

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

#[test]
fn redb_public_api_persists_live_values_and_tombstones_after_reopen() {
    block_on(async {
        let path = std::env::temp_dir().join(format!("kv-{}.db", Uuid::now_v7()));
        let db = Arc::new(Database::create(&path).unwrap());
        let table = "kv-data";
        let tx = db.begin_write().unwrap();
        tx.open_table(table_definition::<Bytes, Vec<u8>>(table))
            .unwrap();
        tx.commit().unwrap();
        let store = KvStore::new(
            RedbByteBTree::new(db.clone(), table),
            test_timestamp_provider,
        );

        let mut write = store.transaction().await.unwrap();
        write
            .set("live", Value::Blob(vec![1, 2]), None)
            .await
            .unwrap();
        write
            .set("deleted", Value::Blob(vec![3]), None)
            .await
            .unwrap();
        write.delete("deleted").await.unwrap();
        write
            .set("expired", Value::Blob(vec![4]), Some(10))
            .await
            .unwrap();
        write
            .set("other", Value::Blob(vec![5]), None)
            .await
            .unwrap();
        write
            .set("recreated", Value::Blob(vec![6]), None)
            .await
            .unwrap();
        write.delete("recreated").await.unwrap();
        write
            .set("recreated", Value::Blob(vec![7]), None)
            .await
            .unwrap();
        assert_eq!(
            write.get("recreated", 0).await.unwrap(),
            Some(Value::Blob(vec![7]))
        );
        assert_eq!(write.get("expired", 10).await.unwrap(), None);
        assert_eq!(
            write
                .scan("live".to_string().."other".to_string(), 0)
                .await
                .unwrap(),
            vec![("live".into(), Value::Blob(vec![1, 2]))]
        );
        write.commit().await.unwrap();
        drop(store);
        drop(db);

        let db = Arc::new(Database::open(&path).unwrap());
        let store = KvStore::new(
            RedbByteBTree::new(db.clone(), table),
            test_timestamp_provider,
        );
        let read = store.transaction().await.unwrap();
        assert_eq!(
            read.get("live", 0).await.unwrap(),
            Some(Value::Blob(vec![1, 2]))
        );
        assert_eq!(read.get("deleted", 0).await.unwrap(), None);
        assert_eq!(
            read.get("recreated", 0).await.unwrap(),
            Some(Value::Blob(vec![7]))
        );
        assert_eq!(
            read.get("expired", 9).await.unwrap(),
            Some(Value::Blob(vec![4]))
        );
        assert_eq!(read.get("expired", 10).await.unwrap(), None);
        read.rollback().await.unwrap();
        drop(store);
        drop(db);
        std::fs::remove_file(path).unwrap();
    });
}
