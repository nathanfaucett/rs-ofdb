use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use btree::{BTreeTransaction, StorageInvalidation};
use btree_redb::{Bytes, RedbDatabase};
use futures::{FutureExt, executor::block_on};
use redb::StorageBackend;

#[derive(Debug)]
struct FailingBackend {
    inner: redb::backends::InMemoryBackend,
    fail: Arc<AtomicBool>,
}

impl StorageBackend for FailingBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected durable commit failure"));
        }
        self.inner.sync_data()
    }
}

#[test]
fn durable_failure_never_publishes_and_cancelling_the_caller_after_commit_keeps_work() {
    block_on(async {
        let fail = Arc::new(AtomicBool::new(false));
        let db = RedbDatabase::new(Arc::new(
            redb::Database::builder()
                .create_with_backend(FailingBackend {
                    inner: redb::backends::InMemoryBackend::new(),
                    fail: fail.clone(),
                })
                .expect("database"),
        ));
        let mut changes = db.subscribe_changes();
        changes.acknowledge(changes.select().expect("initial"));
        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        fail.store(true, Ordering::SeqCst);
        assert!(tx.commit().is_err());
        assert!(changes.select().is_none());
        fail.store(false, Ordering::SeqCst);

        let db = database();
        let mut changes = db.subscribe_changes();
        changes.acknowledge(changes.select().expect("initial"));
        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        let caller = async move {
            tx.commit()
                .expect("synchronous persistence and publication");
            std::future::pending::<()>().await;
        };
        assert!(
            caller.now_or_never().is_none(),
            "caller cancelled on its first pending poll"
        );
        assert_eq!(
            changes
                .select()
                .expect("committed work survived cancellation")
                .invalidations(),
            &[StorageInvalidation::Table("items".into())]
        );
    });
}

fn database() -> RedbDatabase {
    RedbDatabase::new(Arc::new(
        redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("memory database"),
    ))
}

#[test]
fn overflow_failure_and_cancelled_work_retry_conservatively() {
    block_on(async {
        let db = database();
        let mut changes = db.subscribe_changes();
        changes.acknowledge(changes.select().expect("initial"));
        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        tx.commit().expect("commit");
        changes.acknowledge(changes.select().expect("table creation"));
        for key in 0..129_u16 {
            let tx = db.transaction().await.expect("transaction");
            tx.table::<Bytes, Bytes>("items")
                .insert(Bytes(key.to_be_bytes().to_vec()), Bytes(vec![1]))
                .await
                .expect("write");
            tx.commit().expect("commit");
        }
        let selected = changes.select().expect("overflow");
        assert_eq!(selected.invalidations(), &[StorageInvalidation::All]);
        drop(selected);
        assert_eq!(
            changes
                .select()
                .expect("retry after cancellation")
                .invalidations(),
            &[StorageInvalidation::All]
        );
        changes.acknowledge(changes.select().expect("successful retry"));
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![250]), Bytes(vec![4]))
            .await
            .expect("post-overflow write");
        tx.commit().expect("commit");
        let selected = changes.select().expect("selective after broad recovery");
        assert_eq!(
            selected.invalidations(),
            &[StorageInvalidation::Key {
                table: "items".into(),
                key: vec![250]
            }]
        );
        changes.acknowledge(selected);
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![255]), Bytes(vec![1]))
            .await
            .expect("write");
        tx.rollback().expect("rollback");
        assert!(changes.select().is_none());
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![254]), Bytes(vec![1]))
            .await
            .expect("write");
        drop(tx);
        assert!(changes.select().is_none());
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![253]), Bytes(vec![1]))
            .await
            .expect("write");
        tx.table::<Bytes, Bytes>("items")
            .remove(&Bytes(vec![253]))
            .await
            .expect("remove");
        tx.commit().expect("net no-op");
        assert!(changes.select().is_none());
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![0; 17000]), Bytes(vec![1]))
            .await
            .expect("large key");
        tx.commit().expect("commit");
        assert_eq!(
            changes.select().expect("oversized batch").invalidations(),
            &[StorageInvalidation::All]
        );
        let mut other = changes.clone();
        assert!(!other.acknowledge(changes.select().expect("wrong consumer")));
        assert_eq!(
            other.select().expect("still initial").invalidations(),
            &[StorageInvalidation::All]
        );
    });
}

#[test]
fn acknowledgement_cannot_hide_a_later_commit_or_another_consumer() {
    block_on(async {
        let db = database();
        let mut a = db.subscribe_changes();
        let mut b = a.clone();
        let initial = a.select().expect("initial broad work");
        assert_eq!(initial.invalidations(), &[StorageInvalidation::All]);
        assert!(a.acknowledge(initial));
        assert!(b.acknowledge(b.select().expect("independent initial work")));
        let tx = db.transaction().await.expect("transaction");
        tx.create_table::<Bytes, Bytes>("items").expect("table");
        tx.commit().expect("commit");
        a.acknowledge(a.select().expect("table creation"));
        b.acknowledge(b.select().expect("table creation"));

        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![1]), Bytes(vec![2]))
            .await
            .expect("write");
        tx.commit().expect("commit");
        let selected = a.select().expect("first write");
        assert_eq!(
            selected.invalidations(),
            &[StorageInvalidation::Key {
                table: "items".into(),
                key: vec![1]
            }]
        );
        let tx = db.transaction().await.expect("transaction");
        tx.table::<Bytes, Bytes>("items")
            .insert(Bytes(vec![3]), Bytes(vec![4]))
            .await
            .expect("write");
        tx.commit().expect("commit");
        assert!(a.acknowledge(selected));
        assert_eq!(
            a.select().expect("later commit retained").invalidations(),
            &[StorageInvalidation::Key {
                table: "items".into(),
                key: vec![3]
            }]
        );
        assert_eq!(
            b.select()
                .expect("independent history")
                .invalidations()
                .len(),
            2
        );
        assert!(
            a.clone()
                .select()
                .expect("new peer")
                .invalidations()
                .contains(&StorageInvalidation::All)
        );
    });
}
