#![cfg(all(feature = "redb", feature = "sql"))]

use ofdb_sql::{Database, ErrorKind};

#[tokio::test]
async fn native_read_budget_bounds_plain_scans_and_aborts_mutations_across_calls() {
    let directory = tempfile::tempdir().expect("isolated database");
    let database = Database::open(directory.path().join("budget.redb")).expect("database");
    let client = database.client();
    client
        .execute_sql(
            "CREATE TABLE records (id TEXT PRIMARY KEY, content TEXT)",
            None,
        )
        .await
        .expect("schema");
    for i in 0..32 {
        client
            .execute_sql(
                &format!(
                    "INSERT INTO records (id, content) VALUES ('{i}', '{}')",
                    "x".repeat(1024)
                ),
                None,
            )
            .await
            .expect("record");
    }
    for query in [
        "SELECT content FROM records",
        "SELECT content FROM records LIMIT 1",
        "SELECT id FROM records WHERE id = 'absent'",
        "UPDATE records SET content = 'denied'",
        "DELETE FROM records",
    ] {
        let mut transaction = client.transaction().await.expect("transaction");
        transaction
            .set_read_limits(10000, 4096)
            .await
            .expect("native budget");
        let error = transaction
            .execute_sql(query, None)
            .await
            .expect_err("budget stops materialization, including filtered/LIMIT scans");
        assert_eq!(error.kind, ErrorKind::LimitExceeded, "{query}: {error}");
        assert!(
            transaction.commit().await.is_err(),
            "exhausted transaction cannot commit"
        );
    }
    let mut transaction = client.transaction().await.expect("transaction");
    transaction
        .set_read_limits(1, 1024 * 1024)
        .await
        .expect("entry budget");
    let error = transaction
        .execute_sql("SELECT id FROM records", None)
        .await
        .expect_err("entry budget includes catalog reads");
    assert_eq!(error.kind, ErrorKind::LimitExceeded);
    transaction.rollback().await.expect("explicit rollback");
    let snapshot = client
        .execute_sql("SELECT content FROM records", None)
        .await
        .expect("unlimited client unaffected");
    assert_eq!(snapshot[0].rows.len(), 32);
    assert!(
        snapshot[0]
            .rows
            .iter()
            .all(|row| row.values == vec![ofdb_sql::Value::Text("x".repeat(1024))])
    );
}
