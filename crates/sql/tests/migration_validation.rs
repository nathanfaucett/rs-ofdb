#![cfg(all(feature = "sql", feature = "in-memory"))]

use ofdb_sql::{Client, Database, ErrorKind, Migration, MigrationPhase, QueryParams, Row, Value};

const FIRST: &str = "UPDATE items SET amount = 20";
const SECOND: &str = "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'";
const PENDING: &str =
    "UPDATE items SET amount = 99; CREATE TABLE forbidden (id INTEGER PRIMARY KEY)";

async fn execute(client: &Client, sql: &str) -> Vec<Row> {
    client.execute_sql(sql, None).await.expect("fixture SQL")[0]
        .rows
        .clone()
}

async fn snapshot(client: &Client) -> Vec<Vec<Row>> {
    let mut state = Vec::new();
    for sql in [
        "SELECT * FROM tables ORDER BY name",
        "SELECT * FROM table_fields ORDER BY table, position",
        "SELECT * FROM items ORDER BY id",
    ] {
        state.push(execute(client, sql).await);
    }
    if !execute(
        client,
        "SELECT name FROM tables WHERE name = '_ofdb_migrations'",
    )
    .await
    .is_empty()
    {
        state.push(execute(client, "SELECT * FROM _ofdb_migrations ORDER BY version").await);
    }
    state
}

async fn reject(client: &Client, plan: &[Migration], phase: MigrationPhase, version: Option<u32>) {
    let before = snapshot(client).await;
    let error = client
        .migrate(plan)
        .await
        .expect_err("reject invalid plan or history");
    assert_eq!(error.phase, phase);
    assert_eq!(error.version, version);
    assert_eq!(error.source.kind, ErrorKind::Validation);
    assert!(!error.commit_outcome_unknown());
    for message in [error.to_string(), format!("{error:?}")] {
        assert!(!message.contains("private-value"));
        assert!(!message.contains("UPDATE"));
    }
    assert_eq!(
        snapshot(client).await,
        before,
        "rejection must preserve schema, data and history"
    );
}

#[test]
fn invalid_plan_preserves_translation_detail_with_safe_migration_formatting() {
    futures::executor::block_on(async {
        let client = Database::in_memory().client();
        let sql = "INVALID private-value";
        let query_error = client
            .execute_sql(sql, None)
            .await
            .expect_err("invalid query");
        let error = client
            .migrate(&[Migration::new(1, "m1", sql)])
            .await
            .expect_err("invalid plan");
        assert_eq!(error.version, Some(1));
        assert_eq!(error.phase, MigrationPhase::Plan);
        assert_eq!(error.source, query_error);
        for message in [error.to_string(), format!("{error:?}")] {
            assert!(!message.contains("INVALID"));
            assert!(!message.contains("private-value"));
        }
    });
}

#[test]
fn timed_plan_validation_uses_the_normal_sql_deadline_runner() {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        time::Duration,
    };

    let client = Database::in_memory()
        .client()
        .with_deadline(Duration::from_secs(1));
    let sql = "INVALID private-value";
    let query = catch_unwind(AssertUnwindSafe(|| {
        futures::executor::block_on(client.execute_sql(sql, None))
    }));
    assert!(query.is_err(), "native timed SQL requires a Tokio timer");
    let migration = catch_unwind(AssertUnwindSafe(|| {
        futures::executor::block_on(client.migrate(&[Migration::new(1, "m1", sql)]))
    }));
    assert!(
        migration.is_err(),
        "native timed plan validation has the same runtime requirement"
    );
}

async fn fixture(client: &Client) {
    execute(client, "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)").await;
}

async fn rejects_invalid_plans(client: &Client) {
    fixture(client).await;
    for sql in [
        "",
        " \n\t",
        ";;;",
        "-- private-value\n",
        "/* private-value */ ;",
        "INVALID private-value",
    ] {
        reject(
            client,
            &[Migration::new(1, "m1", FIRST), Migration::new(2, "m2", sql)],
            MigrationPhase::Plan,
            Some(2),
        )
        .await;
    }
    for (versions, rejected) in [
        (vec![1, 1], 1),
        (vec![5, 3], 3),
        (vec![1, 2, 1], 1),
        (vec![1, 0], 0),
        (vec![10, 20, 15], 15),
    ] {
        let plan: Vec<_> = versions
            .into_iter()
            .map(|version| Migration::new(version, "m", PENDING))
            .collect();
        reject(client, &plan, MigrationPhase::Plan, Some(rejected)).await;
    }
    assert!(
        execute(
            client,
            "SELECT name FROM tables WHERE name = '_ofdb_migrations'"
        )
        .await
        .is_empty()
    );
    // Existing application schema does not imply migration completion.
    let before = snapshot(client).await;
    let error = client
        .migrate(&[Migration::new(
            1,
            "m1",
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
        )])
        .await
        .expect_err("do not infer history");
    assert_eq!(error.phase, MigrationPhase::Execute);
    assert_eq!(error.source.kind, ErrorKind::Rejected);
    assert_eq!(error.version, Some(1));
    assert_eq!(snapshot(client).await, before);
    assert!(
        execute(
            client,
            "SELECT name FROM tables WHERE name = '_ofdb_migrations'"
        )
        .await
        .is_empty()
    );
    execute(client, "DROP TABLE items").await;
}

async fn rejects_changed_and_newer_history(client: &Client) {
    fixture(client).await;
    let plan = [
        Migration::new(1, "m1", FIRST),
        Migration::new(2, "m2", SECOND),
    ];
    client.migrate(&plan).await.expect("initialize history");
    reject(client, &[], MigrationPhase::History, Some(1)).await;
    reject(client, &plan[..1], MigrationPhase::History, Some(2)).await;
    for changed in [
        "UPDATE items SET amount = 21",
        "UPDATE items SET amount = 20 ",
        "UPDATE items SET amount = 20 -- private-value",
    ] {
        reject(
            client,
            &[
                Migration::new(1, "m1", changed),
                Migration::new(2, "m2", SECOND),
                Migration::new(3, "m3", PENDING),
            ],
            MigrationPhase::History,
            Some(1),
        )
        .await;
    }
    let repeated = client
        .migrate(&plan)
        .await
        .expect("original exact SQL remains valid");
    assert!(repeated.applied_versions.is_empty());
    execute(client, "DROP TABLE _ofdb_migrations; DROP TABLE items").await;
}

async fn rejects_malformed_history(client: &Client) {
    for tamper in [
        "UPDATE _ofdb_migrations SET description = NULL WHERE version = 1",
        "UPDATE _ofdb_migrations SET fingerprint = NULL WHERE version = 2",
        "UPDATE _ofdb_migrations SET fingerprint = X'0000' WHERE version = 1",
        "UPDATE _ofdb_migrations SET execution_time = NULL WHERE version = 1",
        "DELETE FROM _ofdb_migrations WHERE version = 1",
        "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES (0,'x',X'00',0)",
        "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES ($1,'x',X'00',0)",
        "DELETE FROM _ofdb_migrations WHERE version = 2; INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES (4294967296,'x',X'00',0)",
    ] {
        fixture(client).await;
        let plan = [
            Migration::new(1, "m1", FIRST),
            Migration::new(2, "m2", SECOND),
            Migration::new(3, "m3", PENDING),
        ];
        client
            .migrate(&plan[..2])
            .await
            .expect("initialize history");
        let params = QueryParams::Positional(vec![Value::Integer(-1)]);
        client
            .execute_sql(tamper, tamper.contains("$1").then_some(&params))
            .await
            .expect("tamper history through public SQL");
        let before = snapshot(client).await;
        let error = client
            .migrate(&plan)
            .await
            .expect_err("malformed history rejected");
        assert_eq!(error.phase, MigrationPhase::History);
        assert_eq!(error.source.kind, ErrorKind::Validation);
        assert!(!error.to_string().contains("private-value"));
        assert_eq!(
            snapshot(client).await,
            before,
            "malformed history is not repaired: {tamper}"
        );
        execute(client, "DROP TABLE _ofdb_migrations; DROP TABLE items").await;
    }
}

async fn rejects_metadata_collisions(client: &Client) {
    for schema in [
        "version INTEGER, fingerprint TEXT PRIMARY KEY",
        "version TEXT PRIMARY KEY, fingerprint TEXT",
        "id UUID, version INTEGER PRIMARY KEY, description TEXT, fingerprint BLOB, execution_time INTEGER",
        "id INTEGER PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint TEXT, execution_time INTEGER",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time TEXT",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER, extra TEXT",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB DEFAULT X'00', execution_time INTEGER",
        "version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER, PRIMARY KEY (version, fingerprint)",
    ] {
        fixture(client).await;
        execute(client, &format!("CREATE TABLE _ofdb_migrations ({schema})")).await;
        if schema == "version INTEGER, fingerprint TEXT PRIMARY KEY" {
            execute(
                client,
                "INSERT INTO _ofdb_migrations VALUES (1,'private-value'),(1,'other-private-value')",
            )
            .await;
        }
        reject(
            client,
            &[Migration::new(1, "m1", PENDING)],
            MigrationPhase::History,
            None,
        )
        .await;
        reject(client, &[], MigrationPhase::History, None).await;
        execute(client, "DROP TABLE _ofdb_migrations; DROP TABLE items").await;
    }
}

async fn rejects_history_changes_with_active_version(client: &Client) {
    for tamper in [
        "ALTER TABLE _ofdb_migrations ADD COLUMN extra TEXT",
        "UPDATE _ofdb_migrations SET description = 'private-value' WHERE version = 1",
    ] {
        fixture(client).await;
        let first = Migration::new(1, "m1", FIRST);
        client
            .migrate(core::slice::from_ref(&first))
            .await
            .expect("initialize history");
        let second = Migration::new(2, "m2", format!("UPDATE items SET amount = 99; {tamper}"));
        reject(
            client,
            &[first.clone(), second],
            MigrationPhase::Record,
            Some(2),
        )
        .await;
        assert!(
            client
                .migrate(&[first])
                .await
                .expect("original history survives")
                .applied_versions
                .is_empty()
        );
        execute(client, "DROP TABLE _ofdb_migrations; DROP TABLE items").await;
    }
}

async fn validation_suite(client: &Client) {
    rejects_history_changes_with_active_version(client).await;
    rejects_invalid_plans(client).await;
    rejects_changed_and_newer_history(client).await;
    rejects_malformed_history(client).await;
    rejects_metadata_collisions(client).await;
}

#[test]
#[ignore]
fn embedded_rejects_invalid_plans_and_history_without_changes() {
    futures::executor::block_on(validation_suite(&Database::in_memory().client()));
}

#[cfg(all(feature = "remote", feature = "server"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_rejects_invalid_plans_and_history_without_changes() {
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
    validation_suite(&client).await;
    drop(client);
    shutdown.send(()).expect("stop server");
    server.await.expect("join server");
}
