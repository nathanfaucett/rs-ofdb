use std::sync::Arc;

use btree_redb::{Bytes, RedbByteBTree, table_definition};
use futures::executor::block_on;
use kv::KvStore;
use redb::Database;
use uuid::Uuid;

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
        let store = KvStore::new(RedbByteBTree::new(db.clone(), table));

        let mut timestamp = 1;
        let mut write = store
            .transaction_with_uuid_generator(move || {
                let id = Uuid::from_u128((timestamp << 80) | (7 << 76) | (2 << 62));
                timestamp += 1;
                id
            })
            .await
            .unwrap();
        write.set("live", vec![1, 2], None).await.unwrap();
        write.set("deleted", vec![3], None).await.unwrap();
        write.delete("deleted").await.unwrap();
        write.set("expired", vec![4], Some(10)).await.unwrap();
        write.set("other", vec![5], None).await.unwrap();
        write.set("recreated", vec![6], None).await.unwrap();
        write.delete("recreated").await.unwrap();
        write.set("recreated", vec![7], None).await.unwrap();
        assert_eq!(write.get("recreated", 0).await.unwrap(), Some(vec![7]));
        assert_eq!(write.get("expired", 10).await.unwrap(), None);
        assert_eq!(
            write
                .scan("live".to_string().."other".to_string(), 0)
                .await
                .unwrap(),
            vec![("live".into(), vec![1, 2])]
        );
        write.commit().await.unwrap();
        drop(store);
        drop(db);

        let db = Arc::new(Database::open(&path).unwrap());
        let store = KvStore::new(RedbByteBTree::new(db.clone(), table));
        let read = store.transaction().await.unwrap();
        assert_eq!(read.get("live", 0).await.unwrap(), Some(vec![1, 2]));
        assert_eq!(read.get("deleted", 0).await.unwrap(), None);
        assert_eq!(read.get("recreated", 0).await.unwrap(), Some(vec![7]));
        assert_eq!(read.get("expired", 9).await.unwrap(), Some(vec![4]));
        assert_eq!(read.get("expired", 10).await.unwrap(), None);
        read.rollback().await.unwrap();
        drop(store);
        drop(db);
        std::fs::remove_file(path).unwrap();
    });
}
