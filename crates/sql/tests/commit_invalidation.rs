#![cfg(all(feature = "sync", feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Database, SqlInvalidation};

#[test]
fn facade_selected_work_keeps_concurrent_edits_and_independent_progress() {
    std::thread::Builder::new().stack_size(16 * 1024 * 1024).spawn(|| {
        futures::executor::block_on(async {
            let databases = vec![Database::in_memory()];
            #[cfg(feature = "redb")]
            let directory = tempfile::tempdir().expect("temporary directory");
            #[cfg(feature = "redb")]
            let databases = {
                let mut databases = databases;
                databases.push(Database::open(directory.path().join("invalidation.redb")).expect("redb database"));
                databases
            };
            for database in databases {
            let client = database.client();
            let mut changes = database.sync_changes().expect("subscription");
            let mut other = changes.clone();
            client.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items (value) VALUES ('one')", None).await.expect("seed");
            let initial = changes.select().expect("initial");
            assert_eq!(database.sync_invalidations(&initial).await.expect("map"), vec![SqlInvalidation::All]);
            changes.acknowledge(initial);
            other.acknowledge(other.select().expect("other initial"));
            client.execute_sql("UPDATE items SET value = 'two'", None).await.expect("edit");
            let selected = changes.select().expect("select before export");
            let rows = database.sync_invalidations(&selected).await.expect("map");
            assert!(matches!(rows.as_slice(), [SqlInvalidation::Row { table, .. }] if table == "items"));
            client.execute_sql("UPDATE items SET value = 'three'", None).await.expect("concurrent edit");
            changes.acknowledge(selected);
            assert!(changes.select().is_some(), "later edit remains pending");
            assert!(other.select().is_some(), "independent peer remains pending");
            }
        });
    }).expect("fixture thread").join().expect("fixture passed");
}
