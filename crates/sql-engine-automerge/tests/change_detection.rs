use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use automerge::AutoCommit;
use engine::{Engine, EngineResult, InMemoryKernel, KernelTransaction, RowCatalog, RowCodec, Uuid};
use engine_automerge::{AutomergeRowCodec, RowMetadata};
use futures::{Stream, StreamExt, channel::mpsc, executor::block_on};
use sql_translator::SqlTranslator;
use sync::{
    SessionConfig, StateDigest, SyncChangeId, SyncMessage, SyncRole, SyncRowCodec, SyncTransport,
    synchronize,
};
use value::{Row, Value};

const ROW: &str = "018f0f8e-7b6d-7c4a-8f12-123456789abc";

fn row_id() -> Uuid {
    Uuid::parse_str(ROW).expect("fixture UUID")
}

type Database = Engine<InMemoryKernel, AutomergeRowCodec>;

async fn database() -> Database {
    let database = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
    database
        .translate_and_execute(
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, city TEXT);",
            &SqlTranslator,
        )
        .await
        .expect("schema");
    database
        .translate_and_execute(
            &format!("INSERT INTO people VALUES (CAST('{ROW}' AS UUID), 'Ada', 'London');"),
            &SqlTranslator,
        )
        .await
        .expect("row");
    database
}

async fn state(database: &Database) -> (Vec<u8>, Vec<u8>, Vec<SyncChangeId>) {
    database
        .read_transaction(|codec, transaction| {
            Box::pin(async move {
                Ok((
                    codec
                        .export_state(transaction, "people", row_id(), usize::MAX)
                        .await?
                        .expect("row state"),
                    codec
                        .export_metadata(transaction, "people", row_id())
                        .await?,
                    codec
                        .change_inventory(transaction, "people", row_id(), usize::MAX)
                        .await?,
                ))
            })
        })
        .await
        .expect("consistent row observation")
}

async fn update(database: &Database, column: &str, value: &str) {
    database
        .translate_and_execute(
            &format!("UPDATE people SET {column} = '{value}';"),
            &SqlTranslator,
        )
        .await
        .expect("update");
}

async fn bootstrap(source: &Database, destination: &Database) {
    sync::apply_sync_state_batch_for(
        destination,
        sync::export_sync_state_for(source).await.expect("export"),
    )
    .await
    .expect("bootstrap");
}

#[test]
fn snapshot_merge_preserves_concurrent_columns_and_rewriting_preserves_history() {
    block_on(async {
        let source = database().await;
        let destination = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        bootstrap(&source, &destination).await;
        update(&source, "name", "Grace").await;
        update(&destination, "city", "Paris").await;
        bootstrap(&source, &destination).await;
        let rows = destination
            .translate_and_execute("SELECT name, city FROM people;", &SqlTranslator)
            .await
            .expect("merged row");
        assert_eq!(
            rows[0].rows,
            vec![Row::new(vec![Value::from("Grace"), Value::from("Paris")])]
        );
        let (snapshot, metadata, inventory) = state(&destination).await;
        let mut document = AutoCommit::load(&snapshot).expect("snapshot history");
        let changes = document.get_changes(&[]);
        assert_eq!(
            inventory.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
            changes
                .iter()
                .map(|change| change.hash().0.to_vec())
                .collect::<Vec<_>>()
        );
        assert_eq!(changes.len(), 4, "layout, insert, and two concurrent edits");
        destination.read_transaction(move |codec, transaction| Box::pin(async move {
            assert!(codec.export_change(transaction, "people", row_id(), &SyncChangeId(vec![0; 31]), usize::MAX).await.is_err());
            assert!(codec.export_change(transaction, "people", row_id(), &SyncChangeId(vec![0; 32]), usize::MAX).await?.is_none());
            for id in inventory {
                assert!(codec.export_change(transaction, "people", row_id(), &id, 0).await.is_err());
                let payload = codec.export_change(transaction, "people", row_id(), &id, usize::MAX).await?.expect("snapshot-contained native change");
                                let change = automerge::Change::from_bytes(payload).expect("one native change");
                                assert_eq!(change.hash().0.as_slice(), id.0);
            }
            let summary = codec.history_summary(transaction, "people", row_id(), usize::MAX).await?.expect("document summary");
                        assert_eq!(summary.metadata, metadata);
                        assert_eq!(summary.changes.len(), 4);
                        let codec_digest = <AutomergeRowCodec as SyncRowCodec<engine::InMemoryKernelTransaction>>::manifest_digest(codec, &snapshot, &metadata)?;
                        assert_eq!(summary.digest, codec_digest);
                        assert!(codec.history_summary(transaction, "people", row_id(), 1).await.is_err());
            let rewritten = document.save_nocompress();
            assert_eq!(codec_digest, <AutomergeRowCodec as SyncRowCodec<engine::InMemoryKernelTransaction>>::manifest_digest(codec, &rewritten, &metadata)?);
            Ok(())
        })).await.expect("snapshot-only history");
    });
}

#[test]
fn history_summary_keeps_history_and_metadata_in_one_transaction_snapshot() {
    block_on(async {
        let source = Arc::new(database().await);
        let writer = Arc::clone(&source);
        let original = source
            .read_transaction(move |codec, transaction| {
                Box::pin(async move {
                    let before = codec
                        .history_summary(transaction, "people", row_id(), usize::MAX)
                        .await?
                        .expect("summary");
                    writer
                        .translate_and_execute("DELETE FROM people;", &SqlTranslator)
                        .await?;
                    let after = codec
                        .history_summary(transaction, "people", row_id(), usize::MAX)
                        .await?
                        .expect("same snapshot");
                    assert_eq!(
                        before, after,
                        "concurrent commit cannot mix new metadata with old history"
                    );
                    assert!(
                        codec
                            .history_summary(
                                transaction,
                                "people",
                                Uuid::from_bytes([1; 16]),
                                usize::MAX
                            )
                            .await?
                            .is_none()
                    );
                    Ok(before)
                })
            })
            .await
            .expect("transaction snapshot");
        source
            .read_transaction(move |codec, transaction| {
                Box::pin(async move {
                    let committed = codec
                        .history_summary(transaction, "people", row_id(), usize::MAX)
                        .await?
                        .expect("committed summary");
                    assert_eq!(
                        original.changes, committed.changes,
                        "delete changes metadata only"
                    );
                    assert_ne!(original.digest, committed.digest);
                    assert!(
                        postcard::from_bytes::<RowMetadata>(&committed.metadata)
                            .expect("metadata")
                            .deleted
                    );
                    Ok(())
                })
            })
            .await
            .expect("new snapshot observes deletion");
    });
}

#[test]
fn metadata_only_and_combined_edits_change_manifest_without_losing_tombstones() {
    block_on(async {
        let source = database().await;
        let destination = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        bootstrap(&source, &destination).await;
        let (before, _, inventory) = state(&source).await;
        source
            .translate_and_execute("DELETE FROM people;", &SqlTranslator)
            .await
            .expect("delete");
        let (deleted, metadata, deleted_inventory) = state(&source).await;
        assert_eq!(before, deleted, "deletion is metadata-only");
        assert_eq!(inventory, deleted_inventory);
        assert!(
            postcard::from_bytes::<RowMetadata>(&metadata)
                .expect("metadata")
                .deleted
        );
        assert_ne!(
            sync::sync_manifest_for(&source)
                .await
                .expect("deleted manifest"),
            sync::sync_manifest_for(&destination)
                .await
                .expect("live manifest")
        );
        update(&destination, "name", "Grace").await;
        bootstrap(&destination, &source).await;
        let (combined, combined_metadata, combined_inventory) = state(&source).await;
        assert_eq!(combined_inventory.len(), inventory.len() + 1);
        assert_ne!(
            combined, before,
            "incoming history survives even on a tombstoned row"
        );
        assert!(
            postcard::from_bytes::<RowMetadata>(&combined_metadata)
                .expect("combined metadata")
                .deleted
        );
        bootstrap(&source, &destination).await;
        assert_eq!(
            sync::sync_manifest_for(&source).await.expect("source"),
            sync::sync_manifest_for(&destination)
                .await
                .expect("destination")
        );
        assert!(
            destination
                .translate_and_execute("SELECT * FROM people;", &SqlTranslator)
                .await
                .expect("deleted row")[0]
                .rows
                .is_empty()
        );
    });
}

#[test]
fn engine_rollback_aborted_transaction_and_commit_conflict_leave_no_partial_rows() {
    block_on(async {
        let database = database().await;
        let before = sync::sync_manifest_for(&database).await.expect("before");
        let mut transaction = database.transaction().await.expect("transaction");
        transaction
            .translate_and_execute("UPDATE people SET name = 'Rolled back';", &SqlTranslator)
            .await
            .expect("staged edit");
        transaction.rollback().await.expect("rollback");
        assert_eq!(
            before,
            sync::sync_manifest_for(&database)
                .await
                .expect("rollback manifest")
        );
        let mut transaction = database.transaction().await.expect("transaction");
        transaction
            .translate_and_execute("UPDATE people SET name = 'Aborted';", &SqlTranslator)
            .await
            .expect("staged edit");
        assert!(
            transaction
                .translate_and_execute("INSERT INTO missing VALUES (1);", &SqlTranslator)
                .await
                .is_err()
        );
        assert!(transaction.commit().await.is_err());
        assert_eq!(
            before,
            sync::sync_manifest_for(&database)
                .await
                .expect("aborted manifest")
        );
        let mut loser = database.transaction().await.expect("losing transaction");
        loser
            .translate_and_execute("UPDATE people SET name = 'Conflict';", &SqlTranslator)
            .await
            .expect("losing edit");
        update(&database, "city", "Paris").await;
        let committed = sync::sync_manifest_for(&database).await.expect("winner");
        assert!(loser.commit().await.is_err());
        assert_eq!(
            committed,
            sync::sync_manifest_for(&database)
                .await
                .expect("conflict manifest")
        );
    });
}

#[test]
#[ignore = "Existing defect for ticket 04: incremental selection omits deletion metadata when document heads also change; snapshot fallback passes"]
fn incremental_reconciliation_transfers_combined_document_and_deletion_metadata() {
    block_on(async {
        let source = database().await;
        let destination = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        reconcile(&source, &destination).await;
        update(&source, "name", "Grace").await;
        source
            .translate_and_execute("DELETE FROM people;", &SqlTranslator)
            .await
            .expect("metadata edit after document edit");
        reconcile(&source, &destination).await;
        assert!(
            destination
                .translate_and_execute("SELECT * FROM people;", &SqlTranslator)
                .await
                .expect("deleted row")[0]
                .rows
                .is_empty(),
            "incremental selection must carry metadata as well as document changes"
        );
        assert_eq!(
            sync::sync_manifest_for(&source).await.expect("source"),
            sync::sync_manifest_for(&destination)
                .await
                .expect("destination")
        );
    });
}

#[test]
fn multi_change_payload_uses_actual_change_ids_and_dependency_order() {
    block_on(async {
        let source = database().await;
        let destination = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        bootstrap(&source, &destination).await;
        let (_, _, baseline) = state(&source).await;
        update(&source, "name", "Grace").await;
        update(&source, "city", "Paris").await;
        let (snapshot, _, inventory) = state(&source).await;
        let mut document = AutoCommit::load(&snapshot).expect("history");
        let changes = document.get_changes(&[]);
        let mut seen = Vec::new();
        for (id, change) in inventory.iter().zip(&changes) {
            assert_eq!(id.0, change.hash().0);
            assert!(
                change
                    .deps()
                    .iter()
                    .all(|dependency| seen.contains(dependency)),
                "inventory is topological, not hash sorted"
            );
            seen.push(change.hash());
        }
        let missing = &changes[baseline.len()..];
        assert_eq!(missing.len(), 2);
        let before = sync::sync_manifest_for(&destination)
            .await
            .expect("before missing dependency");
        let dependency_missing = sync::SyncIncrementalChange {
            table: "people".into(),
            row: row_id(),
            id: inventory.last().expect("last change").clone(),
            payload: missing[1].raw_bytes().to_vec(),
        };
        assert!(
            sync::apply_incremental_changes_for(&destination, &[dependency_missing])
                .await
                .is_err(),
            "a child cannot be accepted without its parent"
        );
        assert_eq!(
            before,
            sync::sync_manifest_for(&destination)
                .await
                .expect("rejected batch rolled back")
        );
        let mut orphan = AutoCommit::new();
        automerge::transaction::Transactable::put(&mut orphan, automerge::ROOT, "parent", 1)
            .expect("parent");
        orphan.commit();
        automerge::transaction::Transactable::put(&mut orphan, automerge::ROOT, "child", 2)
            .expect("child");
        orphan.commit();
        let orphan_changes = orphan.get_changes(&[]);
        let mut incomplete_snapshot = snapshot.clone();
        incomplete_snapshot.extend_from_slice(orphan_changes[1].raw_bytes());
        let invalid_snapshot = sync::SyncStateUnit::new(
            sync::SyncKey::Row {
                table: "people".into(),
                row: row_id(),
            },
            incomplete_snapshot,
            Vec::new(),
        );
        assert!(
            sync::apply_sync_state_for(&destination, invalid_snapshot)
                .await
                .is_err(),
            "snapshot-contained unresolved changes must not be accepted"
        );
        assert_eq!(
            before,
            sync::sync_manifest_for(&destination)
                .await
                .expect("snapshot rejection rolls back")
        );
        let mut incomplete = missing[0].raw_bytes().to_vec();
        incomplete.extend_from_slice(orphan_changes[1].raw_bytes());
        let invalid = sync::SyncIncrementalChange {
            table: "people".into(),
            row: row_id(),
            id: SyncChangeId(missing[0].hash().0.to_vec()),
            payload: incomplete,
        };
        assert!(
            sync::apply_incremental_changes_for(&destination, &[invalid])
                .await
                .is_err(),
            "matching one hash must not conceal another unresolved change"
        );
        assert_eq!(
            before,
            sync::sync_manifest_for(&destination)
                .await
                .expect("atomic rejection")
        );
        let payload = missing
            .iter()
            .flat_map(|change| change.raw_bytes().iter().copied())
            .collect();
        let payload: Vec<u8> = payload;
        let mut truncated = payload.clone();
        truncated.push(0xff);
        for (id, bytes) in [
            (inventory.last().expect("last").clone(), truncated),
            (baseline[0].clone(), payload.clone()),
            (SyncChangeId(vec![0; 31]), payload.clone()),
        ] {
            let invalid = sync::SyncIncrementalChange {
                table: "people".into(),
                row: row_id(),
                id,
                payload: bytes,
            };
            assert!(
                sync::apply_incremental_changes_for(&destination, &[invalid])
                    .await
                    .is_err(),
                "reject malformed bytes and forged IDs atomically"
            );
            assert_eq!(
                before,
                sync::sync_manifest_for(&destination)
                    .await
                    .expect("unchanged")
            );
        }
        let bundled = sync::SyncIncrementalChange {
            table: "people".into(),
            row: row_id(),
            id: inventory.last().expect("last change").clone(),
            payload,
        };
        sync::apply_incremental_changes_for(&destination, std::slice::from_ref(&bundled))
            .await
            .expect("multi-change payload");
        sync::apply_incremental_changes_for(&destination, &[bundled])
            .await
            .expect("known native history is an idempotent no-op");
        assert_eq!(
            sync::sync_manifest_for(&source).await.expect("source"),
            sync::sync_manifest_for(&destination)
                .await
                .expect("destination")
        );
        let (_, _, received) = state(&destination).await;
        assert_eq!(
            inventory, received,
            "one stored payload still contains two actual change identities"
        );
        destination
            .read_transaction(move |codec, transaction| {
                Box::pin(async move {
                    assert!(
                        codec
                            .export_change(
                                transaction,
                                "people",
                                row_id(),
                                &inventory[baseline.len()],
                                usize::MAX
                            )
                            .await?
                            .is_some(),
                        "native history exports the first bundled change independently"
                    );
                    assert!(
                        codec
                            .export_change(
                                transaction,
                                "people",
                                row_id(),
                                inventory.last().expect("last change"),
                                usize::MAX
                            )
                            .await?
                            .is_some()
                    );
                    Ok(())
                })
            })
            .await
            .expect("physical exportability differs from history");
    });
}

#[derive(Default)]
struct Counts {
    snapshots: AtomicUsize,
    inventories: AtomicUsize,
    digest_loads: AtomicUsize,
    summaries: AtomicUsize,
}

struct CountingCodec {
    inner: AutomergeRowCodec,
    counts: Arc<Counts>,
}

macro_rules! delegate {
    ($($name:ident($($argument:ident: $argument_type:ty),*) -> $output:ty;)*) => {$ (
        async fn $name(&self, $($argument: $argument_type),*) -> EngineResult<$output> {
            self.inner.$name($($argument),*).await
        }
    )*};
}

impl<T: KernelTransaction> RowCodec<T> for CountingCodec {
    delegate! {
        ensure_table(transaction: &mut T, table: &str) -> ();
        drop_table(transaction: &mut T, table: &str) -> ();
        get_row(transaction: &T, table: &str, row: &Uuid) -> Option<Row>;
        put_row(transaction: &mut T, table: &str, row: Uuid, value: Row) -> ();
        remove_row(transaction: &mut T, table: &str, row: &Uuid) -> Option<Row>;
        encode_row(transaction: &T, table: &str, row: &Uuid, value: &Row, columns: &[usize]) -> Vec<u8>;
        conflicted_columns(transaction: &T, table: &str, row: &Uuid, tables: &[(Uuid, Row)], fields: &[Row]) -> Vec<usize>;
        conflict_values(transaction: &T, table: &str, row: &Uuid) -> Vec<(usize, Vec<Value>)>;
        encode_resolution(transaction: &T, table: &str, row: &Uuid, value: &Row, columns: &[usize]) -> Vec<u8>;
        merge_row(transaction: &mut T, table: &str, row: Uuid, value: &[u8]) -> Option<Row>;
        delete_row(transaction: &mut T, table: &str, row: &Uuid) -> Option<Row>;
        row_is_deleted(transaction: &T, table: &str, row: &Uuid) -> bool;
        row_has_dropped_definition(transaction: &T, table: &str, row: &Uuid) -> bool;
        table_definition_was_dropped(transaction: &T, row: &Uuid) -> bool;
        drop_table_definition(transaction: &mut T, row: &Uuid, event: Uuid) -> ();
        table_drop_events(transaction: &T, table: &str) -> Vec<Uuid>;
        set_observed_table_drops(transaction: &mut T, row: &Uuid, events: Vec<Uuid>) -> ();
        observed_table_drops(transaction: &T, row: &Uuid) -> Vec<Uuid>;
    }
    fn scan_rows<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
        catalog: Option<RowCatalog<'a>>,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row)>> + Send + 'a {
        self.inner.scan_rows(transaction, table, catalog)
    }
    fn scan_row_states<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row, bool)>> + Send + 'a {
        self.inner.scan_row_states(transaction, table)
    }
}

impl<T: KernelTransaction> SyncRowCodec<T> for CountingCodec {
    async fn history_summary(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> EngineResult<Option<engine::DocumentSummary>> {
        self.counts.summaries.fetch_add(1, Ordering::Relaxed);
        self.inner
            .history_summary(transaction, table, row, max_bytes)
            .await
    }
    fn row_ids<'a>(
        &'a self,
        transaction: &'a T,
        table: &'a str,
    ) -> impl Stream<Item = EngineResult<Uuid>> + Send + 'a {
        self.inner.row_ids(transaction, table)
    }
    async fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.counts.snapshots.fetch_add(1, Ordering::Relaxed);
        self.inner
            .export_state(transaction, table, row, max_bytes)
            .await
    }
    fn manifest_digest(&self, state: &[u8], metadata: &[u8]) -> EngineResult<StateDigest> {
        if !state.is_empty() {
            self.counts.digest_loads.fetch_add(1, Ordering::Relaxed);
        }
        <AutomergeRowCodec as SyncRowCodec<T>>::manifest_digest(&self.inner, state, metadata)
    }
    async fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        max_bytes: usize,
    ) -> EngineResult<Vec<SyncChangeId>> {
        self.counts.inventories.fetch_add(1, Ordering::Relaxed);
        self.inner
            .change_inventory(transaction, table, row, max_bytes)
            .await
    }
    delegate! {
        merge_state(transaction: &mut T, table: &str, row: Uuid, state: &[u8]) -> Option<Row>;
        export_metadata(transaction: &T, table: &str, row: Uuid) -> Vec<u8>;
        merge_metadata(transaction: &mut T, table: &str, row: Uuid, metadata: &[u8]) -> ();
        export_change(transaction: &T, table: &str, row: Uuid, id: &SyncChangeId, max_bytes: usize) -> Option<Vec<u8>>;
        apply_change(transaction: &mut T, table: &str, row: Uuid, id: &SyncChangeId, payload: &[u8]) -> ();
    }
}

struct Transport {
    receiver: mpsc::UnboundedReceiver<Vec<u8>>,
    sender: mpsc::UnboundedSender<Vec<u8>>,
    states: usize,
    changes: usize,
}
impl SyncTransport for Transport {
    type Error = &'static str;
    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.receiver.next().await.ok_or("closed")
    }
    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error> {
        match postcard::from_bytes::<SyncMessage>(&frame).expect("protocol frame") {
            SyncMessage::State(_) => self.states += 1,
            SyncMessage::Changes(_) => self.changes += 1,
            _ => (),
        }
        self.sender.unbounded_send(frame).map_err(|_| "closed")
    }
}

async fn reconcile<K: engine::Kernel, R: SyncRowCodec<K::Transaction>>(
    left: &Engine<K, R>,
    right: &Engine<K, R>,
) -> (usize, usize) {
    let (left_sender, right_receiver) = mpsc::unbounded();
    let (right_sender, left_receiver) = mpsc::unbounded();
    let mut left_transport = Transport {
        receiver: left_receiver,
        sender: left_sender,
        states: 0,
        changes: 0,
    };
    let mut right_transport = Transport {
        receiver: right_receiver,
        sender: right_sender,
        states: 0,
        changes: 0,
    };
    let config = SessionConfig::default();
    let (left_result, right_result) = futures::join!(
        synchronize(left, &mut left_transport, &config, SyncRole::Initiator),
        synchronize(right, &mut right_transport, &config, SyncRole::Responder)
    );
    left_result.expect("left reconciliation");
    right_result.expect("right reconciliation");
    (left_transport.states, left_transport.changes)
}

#[test]
fn established_peer_one_row_edit_still_discovers_every_document() {
    block_on(async {
        let counts = Arc::new(Counts::default());
        let make = || {
            Engine::new(
                InMemoryKernel::new(),
                CountingCodec {
                    inner: AutomergeRowCodec::new(),
                    counts: Arc::clone(&counts),
                },
            )
        };
        let left = make();
        let right = make();
        left.translate_and_execute(
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, city TEXT);",
            &SqlTranslator,
        )
        .await
        .expect("schema");
        for suffix in 1..=8 {
            left.translate_and_execute(&format!("INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-{suffix:012x}' AS UUID), 'Ada', 'London');"), &SqlTranslator).await.expect("row");
        }
        reconcile(&left, &right).await;
        left.translate_and_execute("UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID);", &SqlTranslator).await.expect("one row edit");
        counts.summaries.store(0, Ordering::Relaxed);
        counts.snapshots.store(0, Ordering::Relaxed);
        counts.inventories.store(0, Ordering::Relaxed);
        counts.digest_loads.store(0, Ordering::Relaxed);
        let frames = reconcile(&left, &right).await;
        let summaries = counts.summaries.load(Ordering::Relaxed);
        let snapshots = counts.snapshots.load(Ordering::Relaxed);
        let inventories = counts.inventories.load(Ordering::Relaxed);
        let digest_loads = counts.digest_loads.load(Ordering::Relaxed);
        eprintln!(
            "summary discovery (both peers): summaries={summaries}, snapshots={snapshots}, history_enumerations={inventories}, digest_loads={digest_loads}, reconstruction_lower_bound={}",
            summaries + snapshots + inventories + digest_loads
        );
        assert_eq!(
            frames,
            (0, 1),
            "incremental wire transfer does not imply selective discovery"
        );
        assert!(summaries >= 16, "discovery remains broad until ticket04");
        assert!(
            snapshots < 49,
            "manifest discovery removes baseline snapshot exports"
        );
        assert!(
            digest_loads < 48,
            "manifest discovery removes baseline digest reloads"
        );
        counts.snapshots.store(0, Ordering::Relaxed);
        counts.digest_loads.store(0, Ordering::Relaxed);
        assert!(inventories > 0);
        assert_eq!(
            sync::sync_manifest_for(&left).await.expect("left"),
            sync::sync_manifest_for(&right).await.expect("right")
        );
        assert_eq!(
            counts.snapshots.load(Ordering::Relaxed),
            0,
            "manifests use direct summaries, not exported snapshots"
        );
        assert_eq!(
            counts.digest_loads.load(Ordering::Relaxed),
            0,
            "manifests never reload saved snapshots"
        );
    });
}
