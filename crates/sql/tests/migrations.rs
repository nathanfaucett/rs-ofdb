#![cfg(all(feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Client, Database, Migration, Row, Value};

const INITIAL: &str =
    "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1, 10)";
const UPGRADE: &str = "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'; UPDATE items SET amount = 20 WHERE id = 1";

async fn initialize_and_upgrade(client: &Client) {
    let empty = client.migrate(&[]).await.expect("empty plan");
    assert_eq!(empty.current_version, 0);
    assert!(empty.applied_versions.is_empty());
    let tables = client
        .execute_sql(
            "SELECT name FROM tables WHERE name = '_ofdb_migrations'",
            None,
        )
        .await
        .expect("inspect metadata absence");
    assert!(tables[0].rows.is_empty());

    let plan = [Migration::new(1, INITIAL), Migration::new(2, UPGRADE)];
    let first = client.migrate(&plan[..1]).await.expect("initialize schema");
    assert_eq!(first.current_version, 1);
    assert_eq!(first.applied_versions, vec![1]);
    let next = client.migrate(&plan).await.expect("upgrade schema");
    assert_eq!(next.current_version, 2);
    assert_eq!(next.applied_versions, vec![2]);
    client
        .execute_sql("UPDATE items SET amount = 30 WHERE id = 1", None)
        .await
        .expect("application changes after upgrade");
    let repeated = client.migrate(&plan).await.expect("skip applied work");
    assert_eq!(repeated.current_version, 2);
    assert!(repeated.applied_versions.is_empty());
    let rows = client
        .execute_sql("SELECT amount, note FROM items", None)
        .await
        .expect("read preserved data");
    assert_eq!(
        rows[0].rows,
        vec![Row::new(vec![Value::Integer(30), Value::from("kept")])]
    );
    let history = client
        .execute_sql(
            "SELECT version FROM _ofdb_migrations ORDER BY version",
            None,
        )
        .await
        .expect("read history");
    assert_eq!(
        history[0].rows,
        vec![Row::from([1_i64]), Row::from([2_i64])]
    );
}

async fn failure_rolls_back_and_stops(client: &Client) {
    let plan = [
        Migration::new(1, INITIAL),
        Migration::new(
            2,
            "UPDATE items SET amount = 99; ALTER TABLE items ADD COLUMN discarded TEXT; INSERT INTO missing VALUES (1)",
        ),
        Migration::new(3, "CREATE TABLE later (id INTEGER PRIMARY KEY)"),
    ];
    let error = client
        .migrate(&plan)
        .await
        .expect_err("second migration fails");
    assert_eq!(error.version, Some(2));
    assert_eq!(error.phase, ofdb_sql::MigrationPhase::Execute);
    assert!(!error.commit_outcome_unknown());
    let rows = client
        .execute_sql("SELECT amount FROM items", None)
        .await
        .expect("earlier migration survives");
    assert_eq!(rows[0].rows, vec![Row::from([10_i64])]);
    assert!(
        client
            .execute_sql("SELECT discarded FROM items", None)
            .await
            .is_err()
    );
    assert!(
        client
            .execute_sql("SELECT id FROM later", None)
            .await
            .is_err()
    );
    let history = client
        .execute_sql("SELECT version FROM _ofdb_migrations", None)
        .await
        .expect("failed migration has no history");
    assert_eq!(history[0].rows, vec![Row::from([1_i64])]);
    let resumed = client
        .migrate(&[Migration::new(1, INITIAL), Migration::new(2, UPGRADE)])
        .await
        .expect("resume after failure");
    assert_eq!(resumed.applied_versions, vec![2]);
}

#[test]
fn failed_batch_rolls_back_schema_data_and_history() {
    futures::executor::block_on(failure_rolls_back_and_stops(
        &Database::in_memory().client(),
    ));
}

#[test]
fn failed_first_migration_does_not_create_metadata() {
    futures::executor::block_on(async {
        let client = Database::in_memory().client();
        client
            .migrate(&[Migration::new(
                1,
                "CREATE TABLE discarded (id INTEGER PRIMARY KEY); INSERT INTO missing VALUES (1)",
            )])
            .await
            .expect_err("first migration fails");
        let result = client
            .execute_sql(
                "SELECT name FROM tables WHERE name = '_ofdb_migrations' OR name = 'discarded'",
                None,
            )
            .await
            .expect("no partial schema");
        assert!(result[0].rows.is_empty());
        assert_eq!(
            client
                .migrate(&[Migration::new(1, INITIAL)])
                .await
                .expect("retry after rollback")
                .applied_versions,
            vec![1]
        );
    });
}

#[test]
fn failed_history_insertion_rolls_back_application_changes() {
    futures::executor::block_on(async {
        let client = Database::in_memory().client();
        let first = "CREATE TABLE items (id INTEGER PRIMARY KEY); CREATE UNIQUE INDEX history_fingerprint ON _ofdb_migrations (fingerprint)";
        let plan = [
            Migration::new(1, first),
            Migration::new(2, "DELETE FROM items"),
            Migration::new(3, "DELETE FROM items"),
        ];
        client
            .migrate(&plan[..2])
            .await
            .expect("initialize two migrations");
        client
            .execute_sql("INSERT INTO items VALUES (7)", None)
            .await
            .expect("application data");
        let error = client
            .migrate(&plan)
            .await
            .expect_err("unique fingerprint prevents completion insert");
        assert_eq!(error.version, Some(3));
        assert_eq!(error.phase, ofdb_sql::MigrationPhase::Record);
        let rows = client
            .execute_sql("SELECT id FROM items", None)
            .await
            .expect("delete rolled back");
        assert_eq!(rows[0].rows, vec![Row::from([7_i64])]);
        let history = client
            .execute_sql(
                "SELECT version FROM _ofdb_migrations ORDER BY version",
                None,
            )
            .await
            .expect("no false completion");
        assert_eq!(
            history[0].rows,
            vec![Row::from([1_i64]), Row::from([2_i64])]
        );
    });
}

#[cfg(feature = "redb")]
#[test]
fn history_and_upgrades_survive_reopening() {
    futures::executor::block_on(async {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("migrations.redb");
        {
            let database = Database::open(&path).expect("open database");
            initialize_and_upgrade(&database.client()).await;
        }
        let database = Database::open(&path).expect("reopen database");
        let client = database.client();
        let result = client
            .migrate(&[Migration::new(1, INITIAL), Migration::new(2, UPGRADE)])
            .await
            .expect("persisted history");
        assert!(result.applied_versions.is_empty());
        assert_eq!(result.current_version, 2);
        let rows = client
            .execute_sql("SELECT amount, note FROM items", None)
            .await
            .expect("persisted data");
        assert_eq!(
            rows[0].rows,
            vec![Row::new(vec![Value::Integer(30), Value::from("kept")])]
        );
    });
}

#[cfg(all(feature = "remote", feature = "server"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_uses_the_same_ordered_runner_and_rollback() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let address = listener.local_addr().expect("local address");
    drop(listener);
    let database = Database::in_memory();
    let (shutdown, signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        database
            .serve_tcp(address, async {
                let _ = signal.await;
            })
            .await
            .expect("serve database");
    });
    let client = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(client) = Client::connect(format!("http://{address}")).await {
                break client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server starts");
    initialize_and_upgrade(&client).await;
    client
        .execute_sql("DROP TABLE items; DROP TABLE _ofdb_migrations", None)
        .await
        .expect("fresh rollback fixture");
    failure_rolls_back_and_stops(&client).await;
    drop(client);
    shutdown.send(()).expect("stop server");
    server.await.expect("join server");
}

#[test]
fn embedded_initializes_upgrades_and_skips_applied_migrations() {
    futures::executor::block_on(initialize_and_upgrade(&Database::in_memory().client()));
}
