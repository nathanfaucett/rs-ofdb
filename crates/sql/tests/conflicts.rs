#![cfg(all(
    any(feature = "redb", feature = "in-memory"),
    feature = "sync",
    feature = "sql"
))]

use ofdb_sql::{
    Database, QueryErrorKind, Row, SessionConfig, SyncRole, SyncTransport, Uuid, Value,
};
use tokio::sync::mpsc;

const ID: &str = "018f0f8e-7b6d-7c4a-8f12-123456789abc";

fn key() -> Row {
    Row::new(vec![Value::Uuid(Uuid::parse_str(ID).expect("row UUID"))])
}

fn check_conflicts_on_two_mib_worker(left: Database, right: Database) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("two MiB worker runtime");
    // Sync fixture construction is separate from the two MiB conflict API worker under test.
    let (runtime, left, transaction) = std::thread::Builder::new()
        .name("conflict-fixture".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            runtime.block_on(diverge(&left, &right));
            let transaction = runtime
                .block_on(left.client().transaction())
                .expect("conflict snapshot");
            (runtime, left, transaction)
        })
        .expect("fixture thread")
        .join()
        .expect("fixture prepared");
    runtime.block_on(async move {
        tokio::spawn(async move {
            let client = left.client();
            let row = key();
            let names = transaction.row_conflicts("security", &row);
            let concurrent = transaction.row_conflict_values("security", &row);
            assert!(core::mem::size_of_val(&names) < 4096);
            assert!(core::mem::size_of_val(&concurrent) < 4096);
            let snapshot_names = names.await.expect("snapshot names on worker");
            let snapshot_values = concurrent.await.expect("snapshot values on worker");
            transaction.rollback().await.expect("snapshot rollback");
            let values = client
                .row_conflict_values("security", &row)
                .await
                .expect("concurrent values on worker");
            assert_eq!(values.len(), 4);
            assert!(values[0].1.contains(&Value::Bool(false)));
            assert!(values[0].1.contains(&Value::Bool(true)));
            let columns = client
                .row_conflicts("security", &row)
                .await
                .expect("conflicted columns on worker");
            assert_eq!(columns.len(), 4);
            assert_eq!(snapshot_names, columns);
            assert_eq!(snapshot_values, values);
            assert_eq!(
                client
                    .row_conflicts_batch("security", &[row.clone(), row.clone()])
                    .await
                    .expect("batch conflicts on worker"),
                vec![columns.clone(), columns]
            );
            client
                .resolve_row(
                    "security",
                    &row,
                    vec![("admin_allowed".into(), Value::Bool(false))],
                )
                .await
                .expect("partial resolution on worker");
            let remaining = client
                .row_conflict_values("security", &row)
                .await
                .expect("remaining concurrent values on worker");
            assert_eq!(remaining, values[1..]);
        })
        .await
        .expect("conflict worker completed");
    });
}

#[cfg(feature = "redb")]
#[test]
fn native_conflicts_on_two_mib_worker() {
    let directory = tempfile::tempdir().expect("databases");
    let left = Database::open(directory.path().join("left.redb")).expect("left");
    let right = Database::open(directory.path().join("right.redb")).expect("right");
    check_conflicts_on_two_mib_worker(left, right);
}

#[cfg(feature = "in-memory")]
#[test]
fn memory_conflicts_on_two_mib_worker() {
    check_conflicts_on_two_mib_worker(Database::in_memory(), Database::in_memory());
}

struct Channel {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}
impl SyncTransport for Channel {
    type Error = &'static str;
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        self.sender.send(frame).map_err(|_| "closed")
    }
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.recv().await.ok_or("closed")
    }
}
async fn exchange(left: &Database, right: &Database) {
    let (a, ar) = mpsc::unbounded_channel();
    let (b, br) = mpsc::unbounded_channel();
    let mut lt = Channel {
        sender: a,
        receiver: br,
    };
    let mut rt = Channel {
        sender: b,
        receiver: ar,
    };
    let config = SessionConfig::default();
    let (sent, received) = tokio::join!(
        left.synchronize(&mut lt, &config, SyncRole::Initiator),
        right.synchronize(&mut rt, &config, SyncRole::Responder)
    );
    sent.expect("left sync");
    received.expect("right sync");
}
#[cfg(feature = "in-memory")]
#[test]
fn memory_conflict_operations_and_transaction_reads_work_without_runtime() {
    futures::executor::block_on(async {
        let left = Database::in_memory();
        let right = Database::in_memory();
        diverge(&left, &right).await;
        let client = left.client();
        let row = key();
        let values = client
            .row_conflict_values("security", &row)
            .await
            .expect("memory values");
        assert_eq!(
            client
                .row_conflicts("security", &row)
                .await
                .expect("memory names"),
            vec![
                "admin_allowed",
                "selected_id",
                "revoked_at",
                "owner_subject"
            ]
        );
        let transaction = client.transaction().await.expect("memory transaction");
        assert_eq!(
            transaction
                .row_conflict_values("security", &row)
                .await
                .expect("memory snapshot values"),
            values
        );
        assert_eq!(
            transaction
                .row_conflicts("security", &row)
                .await
                .expect("memory snapshot names")
                .len(),
            4
        );
        transaction.rollback().await.expect("memory rollback");
        assert_eq!(
            client
                .resolve_row("security", &row, Vec::new())
                .await
                .expect_err("empty resolution rejected")
                .kind,
            QueryErrorKind::Rejected
        );
        client
            .resolve_row(
                "security",
                &row,
                vec![("admin_allowed".into(), Value::Bool(false))],
            )
            .await
            .expect("memory explicit resolution");
        assert_eq!(
            client
                .row_conflicts("security", &row)
                .await
                .expect("remaining conflicts"),
            vec!["selected_id", "revoked_at", "owner_subject"]
        );
    });
}

async fn check_row_conflicts_batch(left: &Database, right: &Database) {
    diverge(left, right).await;
    let client = left.client();
    let absent = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abe").expect("absent UUID"),
    )]);
    let keys = [absent.clone(), key(), key()];
    let names: Vec<String> = [
        "admin_allowed",
        "selected_id",
        "revoked_at",
        "owner_subject",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let before = left
        .export_sync_state()
        .await
        .expect("history before inspection");
    assert_eq!(
        client
            .row_conflicts_batch("security", &keys)
            .await
            .expect("ordered batch"),
        vec![Vec::<String>::new(), names.clone(), names]
    );
    assert_eq!(
        left.export_sync_state().await.expect("unchanged history"),
        before
    );
    assert!(
        client
            .row_conflicts_batch("absent", &[])
            .await
            .expect("empty batch is a no-op")
            .is_empty()
    );
    for (table, keys, single_key) in [
        (
            "absent",
            vec![Row::new(vec![Value::Bool(true)])],
            Row::new(vec![Value::Bool(true)]),
        ),
        (
            "security",
            vec![key(), Row::new(vec![Value::Bool(true)])],
            Row::new(vec![Value::Bool(true)]),
        ),
        (
            "security",
            vec![Row::new(vec![Value::Bool(true)]), Row::new(vec![])],
            Row::new(vec![Value::Bool(true)]),
        ),
    ] {
        let single = client
            .row_conflicts(table, &single_key)
            .await
            .expect_err("single-row rejection");
        let batch = client
            .row_conflicts_batch(table, &keys)
            .await
            .expect_err("same first rejection");
        assert_eq!(batch.kind, single.kind);
        assert_eq!(batch.to_string(), single.to_string());
    }
    client
        .resolve_row(
            "security",
            &key(),
            vec![("admin_allowed".into(), Value::Bool(false))],
        )
        .await
        .expect("resolve between batches");
    assert_eq!(
        client
            .row_conflicts_batch("security", &[key(), absent])
            .await
            .expect("fresh snapshot"),
        vec![
            vec![
                String::from("selected_id"),
                String::from("revoked_at"),
                String::from("owner_subject")
            ],
            vec![]
        ]
    );
    client
        .execute_sql("DROP TABLE security", None)
        .await
        .expect("drop between batches");
    let single = client
        .row_conflicts("security", &key())
        .await
        .expect_err("current catalog");
    let batch = client
        .row_conflicts_batch("security", &[key()])
        .await
        .expect_err("no retained catalog");
    assert_eq!(batch.kind, single.kind);
    assert_eq!(batch.to_string(), single.to_string());
}

#[cfg(feature = "in-memory")]
#[test]
fn row_conflicts_batch_memory_preserves_order_validation_and_freshness() {
    futures::executor::block_on(async {
        check_row_conflicts_batch(&Database::in_memory(), &Database::in_memory()).await;
    });
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn row_conflicts_batch_native_preserves_order_validation_and_freshness() {
    let directory = tempfile::tempdir().expect("databases");
    let left = Database::open(directory.path().join("left.redb")).expect("left");
    let right = Database::open(directory.path().join("right.redb")).expect("right");
    check_row_conflicts_batch(&left, &right).await;
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn unchanged_updates_commit_without_new_history_and_remain_syncable() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("left.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::open(directory.path().join("right.redb")).expect("right");
    left.client()
        .execute_sql(
            "CREATE TABLE revisions (id UUID PRIMARY KEY, revision TEXT)",
            None,
        )
        .await
        .expect("schema");
    left.client()
        .execute_sql(
            &format!("INSERT INTO revisions VALUES (CAST('{ID}' AS UUID), 'same')"),
            None,
        )
        .await
        .expect("row");
    let before = left.export_sync_state().await.expect("before");
    left.client()
        .execute_sql("UPDATE revisions SET revision = 'same'", None)
        .await
        .expect("valid unchanged update");
    let mut tx = left.client().transaction().await.expect("transaction");
    tx.execute_sql("UPDATE revisions SET revision = 'same'", None)
        .await
        .expect("unchanged transactional update");
    tx.commit().await.expect("commit no-op");
    assert_eq!(
        left.export_sync_state().await.expect("unchanged history"),
        before
    );
    drop(left);
    let left = Database::open(&path).expect("reopen no-op");
    assert_eq!(
        left.export_sync_state().await.expect("durable history"),
        before
    );
    exchange(&left, &right).await;
    left.client()
        .execute_sql("UPDATE revisions SET revision = 'changed'", None)
        .await
        .expect("real change");
    exchange(&left, &right).await;
    assert_eq!(
        left.export_sync_state().await.expect("left"),
        right.export_sync_state().await.expect("right")
    );
}

#[cfg(all(feature = "remote", feature = "server"))]
#[test]
fn remote_conflict_inspection_resolution_and_transaction_snapshot() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(2 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("runtime");
            let left = Database::in_memory();
            let right = Database::in_memory();
            runtime.block_on(diverge(&left, &right));
            runtime.block_on(check_remote_conflicts(left));
        })
        .expect("fixture thread")
        .join()
        .expect("remote parity");
}

#[cfg(all(feature = "remote", feature = "server"))]
async fn check_remote_conflicts(left: Database) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve address");
    let address = listener.local_addr().expect("address");
    drop(listener);
    let hosted = left.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        hosted
            .serve_tcp(address, async {
                let _ = stopped.await;
            })
            .await
            .expect("serve SQL")
    });
    let client = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(client) = ofdb_sql::Client::connect(format!("http://{address}")).await {
                break client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded SQL startup");
    let row = key();
    let names = vec![
        "admin_allowed",
        "selected_id",
        "revoked_at",
        "owner_subject",
    ];
    assert_eq!(
        client
            .row_conflicts("security", &row)
            .await
            .expect("remote names"),
        names
    );
    assert_eq!(
        client
            .row_conflicts_batch("security", &[row.clone(), row.clone()])
            .await
            .expect("remote ordered duplicates"),
        vec![names.clone(), names.clone()]
    );
    let values = client
        .row_conflict_values("security", &row)
        .await
        .expect("remote values");
    assert_eq!(values.len(), 4);
    assert!(values[0].1.contains(&Value::Bool(false)));
    assert!(values[0].1.contains(&Value::Bool(true)));
    let mut transaction = client.transaction().await.expect("remote transaction");
    assert_eq!(
        transaction
            .row_conflicts("security", &row)
            .await
            .expect("snapshot names"),
        names
    );
    assert_eq!(
        transaction
            .row_conflict_values("security", &row)
            .await
            .expect("snapshot values"),
        values
    );
    transaction
        .execute_sql("CREATE TABLE pending (id UUID PRIMARY KEY)", None)
        .await
        .expect("transaction-local table");
    assert!(
        transaction
            .row_conflicts("pending", &row)
            .await
            .expect("inspection sees uncommitted catalog")
            .is_empty()
    );
    transaction
        .execute_sql("SELECT * FROM absent", None)
        .await
        .expect_err("abort transaction");
    assert!(transaction.row_conflicts("security", &row).await.is_err());
    assert!(
        transaction
            .row_conflict_values("security", &row)
            .await
            .is_err()
    );
    transaction
        .commit()
        .await
        .expect_err("aborted transaction cannot commit");
    client
        .row_conflicts("pending", &row)
        .await
        .expect_err("failed transaction rolled back");
    client
        .resolve_row(
            "security",
            &row,
            vec![("admin_allowed".into(), Value::Bool(false))],
        )
        .await
        .expect("remote partial resolution");
    assert_eq!(
        client
            .row_conflicts("security", &row)
            .await
            .expect("unresolved columns retained"),
        names[1..]
    );
    client
        .resolve_row(
            "security",
            &row,
            vec![("absent".into(), Value::Bool(false))],
        )
        .await
        .expect_err("invalid resolution fails closed");
    stop.send(()).expect("stop server");
    server.await.expect("SQL server task");
    client
        .row_conflicts("security", &row)
        .await
        .expect_err("unavailable owner must not report clear");
}

async fn diverge(left: &Database, right: &Database) {
    left.client().execute_sql("CREATE TABLE security (id UUID PRIMARY KEY, admin_allowed BOOLEAN, selected_id UUID, revoked_at INTEGER, owner_subject TEXT)", None).await.expect("schema");
    left.client().execute_sql(&format!("INSERT INTO security VALUES (CAST('{ID}' AS UUID), NULL, CAST('{ID}' AS UUID), NULL, 'original')"), None).await.expect("baseline");
    exchange(left, right).await;
    left.client().execute_sql("UPDATE security SET admin_allowed = false, selected_id = NULL, revoked_at = 10, owner_subject = 'alice'", None).await.expect("left offline change");
    right.client().execute_sql("UPDATE security SET admin_allowed = true, selected_id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), revoked_at = 20, owner_subject = 'mallory'", None).await.expect("right offline change");
    exchange(left, right).await;
}

#[cfg(all(feature = "redb", feature = "in-memory"))]
#[tokio::test]
async fn sequential_concurrent_updates_and_partial_resolution_sync_after_reopen() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("left.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::in_memory();
    left.client().execute_sql("CREATE TABLE security (id UUID PRIMARY KEY, admin_allowed BOOLEAN, owner_subject TEXT)", None).await.expect("schema");
    left.client()
        .execute_sql(
            &format!("INSERT INTO security VALUES (CAST('{ID}' AS UUID), true, 'original')"),
            None,
        )
        .await
        .expect("baseline");
    exchange(&left, &right).await;
    left.client()
        .execute_sql(
            "UPDATE security SET admin_allowed = false, owner_subject = 'alice'",
            None,
        )
        .await
        .expect("left offline");
    right
        .client()
        .execute_sql("UPDATE security SET admin_allowed = false", None)
        .await
        .expect("right first update");
    right
        .client()
        .execute_sql(
            "UPDATE security SET admin_allowed = true, owner_subject = 'bob'",
            None,
        )
        .await
        .expect("right second update");
    exchange(&left, &right).await;
    let row = key();
    let values = left
        .client()
        .row_conflict_values("security", &row)
        .await
        .expect("all concurrent values");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].0, "admin_allowed");
    assert!(values[0].1.contains(&Value::Bool(false)));
    assert!(values[0].1.contains(&Value::Bool(true)));
    assert!(values[1].1.contains(&Value::Text("alice".into())));
    assert!(values[1].1.contains(&Value::Text("bob".into())));
    left.client()
        .resolve_row(
            "security",
            &row,
            vec![("admin_allowed".into(), Value::Bool(false))],
        )
        .await
        .expect("partial explicit resolution");
    drop(left);
    let left = Database::open(&path).expect("reopen partial resolution");
    exchange(&left, &right).await;
    for database in [&left, &right] {
        assert_eq!(
            database
                .client()
                .row_conflicts("security", &row)
                .await
                .expect("ownership remains ambiguous"),
            vec!["owner_subject"]
        );
        assert_eq!(
            database
                .client()
                .execute_sql("SELECT admin_allowed FROM security", None)
                .await
                .expect("resolved projection")[0]
                .rows,
            vec![Row::new(vec![Value::Bool(false)])]
        );
    }
    assert_eq!(
        left.export_sync_state().await.expect("left state"),
        right.export_sync_state().await.expect("right state")
    );
    drop(left);
    let left = Database::open(&path).expect("reopen converged history");
    assert_eq!(
        left.client()
            .row_conflict_values("security", &row)
            .await
            .expect("durable unresolved ownership"),
        right
            .client()
            .row_conflict_values("security", &row)
            .await
            .expect("peer ownership")
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn transaction_conflict_checks_use_local_snapshot_and_reject_aborted_state() {
    let directory = tempfile::tempdir().expect("databases");
    let left = Database::open(directory.path().join("left.redb")).expect("left");
    let right = Database::open(directory.path().join("right.redb")).expect("right");
    diverge(&left, &right).await;
    let client = left.client();
    let row = key();
    let names = client.row_conflicts("security", &row).await.expect("names");
    let values = client
        .row_conflict_values("security", &row)
        .await
        .expect("values");
    let mut transaction = client.transaction().await.expect("transaction");
    assert_eq!(
        transaction
            .row_conflicts("security", &row)
            .await
            .expect("same snapshot names"),
        names
    );
    assert_eq!(
        transaction
            .row_conflict_values("security", &row)
            .await
            .expect("same snapshot values"),
        values
    );
    transaction.execute_sql(&format!("CREATE TABLE local_only (id UUID PRIMARY KEY); INSERT INTO local_only VALUES (CAST('{ID}' AS UUID))"), None).await.expect("transaction local catalog and row");
    assert!(
        transaction
            .row_conflicts("local_only", &row)
            .await
            .expect("local catalog exists in snapshot")
            .is_empty()
    );
    assert!(
        transaction
            .row_conflict_values("local_only", &row)
            .await
            .expect("local row clear")
            .is_empty()
    );
    transaction.rollback().await.expect("rollback local writes");
    assert_eq!(
        client
            .row_conflicts("local_only", &row)
            .await
            .expect_err("rolled back catalog")
            .kind,
        QueryErrorKind::Rejected
    );
    let mut transaction = client.transaction().await.expect("aborted transaction");
    transaction
        .execute_sql("CREATE TABLE rollback_marker (id UUID PRIMARY KEY)", None)
        .await
        .expect("pending mutation");
    transaction
        .execute_sql("SELECT * FROM absent", None)
        .await
        .expect_err("abort transaction");
    assert_eq!(
        transaction
            .row_conflicts("security", &row)
            .await
            .expect_err("not a clear result after abort")
            .kind,
        QueryErrorKind::Rejected
    );
    assert_eq!(
        transaction
            .row_conflict_values("security", &row)
            .await
            .expect_err("not an empty result after abort")
            .kind,
        QueryErrorKind::Rejected
    );
    transaction
        .commit()
        .await
        .expect_err("aborted commit rolls back");
    assert!(
        client
            .execute_sql("SELECT * FROM rollback_marker", None)
            .await
            .is_err()
    );
    assert_eq!(
        client
            .row_conflict_values("security", &row)
            .await
            .expect("original conflicts remain"),
        values
    );
    drop(client);
    drop(left);
    let reopened =
        Database::open(directory.path().join("left.redb")).expect("reopen after rollback");
    assert_eq!(
        reopened
            .client()
            .row_conflict_values("security", &row)
            .await
            .expect("persisted conflicts"),
        values
    );
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn native_conflicts_and_explicit_resolution_survive_reopen_and_converge() {
    let directory = tempfile::tempdir().expect("databases");
    let path = directory.path().join("left.redb");
    let left = Database::open(&path).expect("left");
    let right = Database::open(directory.path().join("right.redb")).expect("right");
    diverge(&left, &right).await;
    let row = key();
    let columns = vec![
        "admin_allowed",
        "selected_id",
        "revoked_at",
        "owner_subject",
    ];
    assert_eq!(
        left.client()
            .row_conflicts("security", &row)
            .await
            .expect("conflicts"),
        columns
    );
    let values = left
        .client()
        .row_conflict_values("security", &row)
        .await
        .expect("values");
    assert_eq!(values.len(), 4);
    assert_eq!(values[0].0, "admin_allowed");
    assert_eq!(values[0].1.len(), 2);
    assert!(values[0].1.contains(&Value::Bool(false)));
    assert!(values[0].1.contains(&Value::Bool(true)));
    assert!(values[1].1.contains(&Value::Null));
    assert!(values[1].1.contains(&Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd").expect("selected UUID")
    )));
    assert!(values[2].1.contains(&Value::Integer(10)));
    assert!(values[2].1.contains(&Value::Integer(20)));
    assert!(values[3].1.contains(&Value::Text("alice".into())));
    assert!(values[3].1.contains(&Value::Text("mallory".into())));
    drop(left);
    let left = Database::open(&path).expect("reopen conflicts");
    let client = left.client();
    assert_eq!(
        client
            .row_conflict_values("security", &row)
            .await
            .expect("durable values"),
        values
    );
    let absent = Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abe").expect("absent UUID"),
    )]);
    assert!(
        client
            .row_conflicts("security", &absent)
            .await
            .expect("missing row is not existence proof")
            .is_empty()
    );
    assert!(
        client
            .row_conflict_values("security", &absent)
            .await
            .expect("no missing-row conflicts")
            .is_empty()
    );
    assert_eq!(
        client
            .resolve_row(
                "security",
                &absent,
                vec![("admin_allowed".into(), Value::Bool(false))]
            )
            .await
            .expect_err("missing row")
            .kind,
        QueryErrorKind::Rejected
    );
    let invalid_key = Row::new(vec![Value::Integer(1)]);
    assert_eq!(
        client
            .row_conflicts("security", &invalid_key)
            .await
            .expect_err("invalid key")
            .kind,
        QueryErrorKind::Rejected
    );
    assert_eq!(
        client
            .row_conflict_values("security", &invalid_key)
            .await
            .expect_err("invalid key values")
            .kind,
        QueryErrorKind::Rejected
    );
    assert_eq!(
        client
            .resolve_row(
                "security",
                &invalid_key,
                vec![("admin_allowed".into(), Value::Bool(false))]
            )
            .await
            .expect_err("invalid resolution key")
            .kind,
        QueryErrorKind::Rejected
    );
    assert_eq!(
        client
            .row_conflicts("absent", &row)
            .await
            .expect_err("missing table")
            .kind,
        QueryErrorKind::Rejected
    );
    assert_eq!(
        client
            .row_conflict_values("absent", &row)
            .await
            .expect_err("missing table values")
            .kind,
        QueryErrorKind::Rejected
    );
    for assignments in [
        vec![],
        vec![("missing".into(), Value::Null)],
        vec![("id".into(), row.values[0].clone())],
        vec![
            ("admin_allowed".into(), Value::Bool(false)),
            ("admin_allowed".into(), Value::Bool(true)),
        ],
        vec![
            ("admin_allowed".into(), Value::Bool(false)),
            ("missing".into(), Value::Null),
        ],
    ] {
        assert_eq!(
            client
                .resolve_row("security", &row, assignments)
                .await
                .expect_err("invalid resolution")
                .kind,
            QueryErrorKind::Rejected
        );
        assert_eq!(
            client
                .row_conflict_values("security", &row)
                .await
                .expect("failed resolution unchanged"),
            values
        );
    }
    client
        .resolve_row(
            "security",
            &row,
            vec![
                ("admin_allowed".into(), Value::Bool(false)),
                ("selected_id".into(), Value::Null),
                ("revoked_at".into(), Value::Integer(20)),
            ],
        )
        .await
        .expect("caller-selected restrictive values");
    assert_eq!(
        client
            .row_conflicts("security", &row)
            .await
            .expect("no implicit ownership resolution"),
        vec!["owner_subject"]
    );
    assert_eq!(
        client
            .resolve_row(
                "security",
                &row,
                vec![("admin_allowed".into(), Value::Bool(true))]
            )
            .await
            .expect_err("nonconflicted column")
            .kind,
        QueryErrorKind::Rejected
    );
    client
        .resolve_row(
            "security",
            &row,
            vec![("owner_subject".into(), Value::Text("alice".into()))],
        )
        .await
        .expect("explicit ownership resolution");
    assert!(
        client
            .row_conflicts("security", &row)
            .await
            .expect("resolved")
            .is_empty()
    );
    drop(client);
    drop(left);
    let left = Database::open(&path).expect("reopen resolution");
    let result = left
        .client()
        .execute_sql(
            "SELECT admin_allowed, selected_id, revoked_at, owner_subject FROM security",
            None,
        )
        .await
        .expect("resolved projection");
    assert_eq!(
        result[0].rows,
        vec![Row::new(vec![
            Value::Bool(false),
            Value::Null,
            Value::Integer(20),
            Value::Text("alice".into())
        ])]
    );
    exchange(&left, &right).await;
    assert!(
        right
            .client()
            .row_conflict_values("security", &row)
            .await
            .expect("peer resolved")
            .is_empty()
    );
    assert_eq!(
        left.export_sync_state().await.expect("left state"),
        right.export_sync_state().await.expect("right state")
    );
    left.client()
        .execute_sql("DELETE FROM security", None)
        .await
        .expect("tombstone");
    assert_eq!(
        left.client()
            .resolve_row(
                "security",
                &row,
                vec![("owner_subject".into(), Value::Text("resurrected".into()))]
            )
            .await
            .expect_err("cannot resurrect")
            .kind,
        QueryErrorKind::Rejected
    );
    assert!(
        left.client()
            .execute_sql("SELECT * FROM security", None)
            .await
            .expect("still deleted")[0]
            .rows
            .is_empty()
    );
}
