use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use btree::{BTreeRead, BTreeTransaction};
use btree_redb::RedbDatabase;
use futures::{StreamExt, executor::block_on};

static DECODES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct TrackedValue(Vec<u8>);

impl redb::Value for TrackedValue {
    type SelfType<'a> = Self;
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(bytes: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        DECODES.fetch_add(1, Ordering::SeqCst);
        Self(bytes.to_vec())
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> &'a [u8]
    where
        Self: 'b,
    {
        &value.0
    }
    fn type_name() -> redb::TypeName {
        redb::TypeName::new("budget-test-value")
    }
}

#[test]
fn budget_rejection_precedes_owned_value_decoding_for_get_range_and_mutation_reads() {
    block_on(async {
        let directory = tempfile::tempdir().expect("isolated native database");
        let database = RedbDatabase::new(Arc::new(
            redb::Database::create(directory.path().join("budget.redb")).expect("database"),
        ));
        let transaction = database.transaction().await.expect("seed transaction");
        transaction
            .table::<u64, TrackedValue>("data")
            .insert(1, TrackedValue(vec![0; 1024 * 1024]))
            .await
            .expect("large native value");
        transaction.commit().expect("seed");
        for operation in ["get", "range", "remove", "remove_range", "insert"] {
            let mut transaction = database.transaction().await.expect("bounded transaction");
            transaction.set_read_limits(10, 1024).expect("read budget");
            assert!(
                transaction.set_read_limits(100, 1024 * 1024).is_err(),
                "budget cannot be reset"
            );
            DECODES.store(0, Ordering::SeqCst);
            let mut table = transaction.table::<u64, TrackedValue>("data");
            match operation {
                "get" => assert!(table.get(&1).await.is_err()),
                "range" => assert!(table.range(..).collect::<Vec<_>>().await[0].is_err()),
                "remove_range" => {
                    assert!(table.remove_range(..).collect::<Vec<_>>().await[0].is_err())
                }
                "remove" => assert!(table.remove(&1).await.is_err()),
                "insert" => assert!(table.insert(1, TrackedValue(vec![])).await.is_err()),
                _ => unreachable!(),
            }
            assert_eq!(
                DECODES.load(Ordering::SeqCst),
                0,
                "{operation} did not allocate/decode the oversized native value"
            );
            transaction.rollback().expect("rollback");
        }
    });
}
