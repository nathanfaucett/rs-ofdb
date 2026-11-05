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
    for message in [
        error.to_string(),
        format!("{error:?}"),
        error.source.to_string(),
    ] {
        assert!(!message.contains("private-value"));
        assert!(!message.contains("UPDATE"));
    }
    assert_eq!(
        snapshot(client).await,
        before,
        "rejection must preserve schema, data and history"
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
            &[Migration::new(1, FIRST), Migration::new(2, sql)],
            MigrationPhase::Plan,
            Some(2),
        )
        .await;
    }
    for (versions, rejected) in [
        (vec![0], 0),
        (vec![2], 2),
        (vec![u32::MAX], u32::MAX),
        (vec![1, 1], 1),
        (vec![1, 3], 3),
        (vec![1, 2, 1], 1),
        (vec![1, 0], 0),
    ] {
        let plan: Vec<_> = versions
            .into_iter()
            .map(|version| Migration::new(version, PENDING))
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
    let plan = [Migration::new(1, FIRST), Migration::new(2, SECOND)];
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
                Migration::new(1, changed),
                Migration::new(2, SECOND),
                Migration::new(3, PENDING),
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
        "UPDATE _ofdb_migrations SET fingerprint = NULL WHERE version = 1",
        "UPDATE _ofdb_migrations SET fingerprint = 'private-value' WHERE version = 2",
        "UPDATE _ofdb_migrations SET fingerprint = '0000000000000000000000000000000000000000000000000000000000000000' WHERE version = 1",
        "DELETE FROM _ofdb_migrations WHERE version = 1",
        "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations VALUES (0,'private-value')",
        "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations VALUES ($1,'private-value')",
        "DELETE FROM _ofdb_migrations WHERE version = 2; INSERT INTO _ofdb_migrations VALUES (4294967296,'private-value')",
    ] {
        fixture(client).await;
        let plan = [
            Migration::new(1, FIRST),
            Migration::new(2, SECOND),
            Migration::new(3, PENDING),
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
        "version INTEGER PRIMARY KEY, fingerprint INTEGER",
        "version INTEGER PRIMARY KEY, fingerprint TEXT, extra TEXT",
        "version INTEGER PRIMARY KEY, fingerprint TEXT DEFAULT 'private-value'",
        "version INTEGER, fingerprint TEXT, PRIMARY KEY (version, fingerprint)",
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
            &[Migration::new(1, PENDING)],
            MigrationPhase::History,
            None,
        )
        .await;
        reject(client, &[], MigrationPhase::History, None).await;
        execute(client, "DROP TABLE _ofdb_migrations; DROP TABLE items").await;
    }
}

async fn validation_suite(client: &Client) {
    rejects_invalid_plans(client).await;
    rejects_changed_and_newer_history(client).await;
    rejects_malformed_history(client).await;
    rejects_metadata_collisions(client).await;
}

#[test]
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
