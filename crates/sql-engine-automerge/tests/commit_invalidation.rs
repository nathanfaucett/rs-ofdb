use engine::{Engine, InMemoryKernel, Kernel, KernelTransaction};
use engine_automerge::{AutomergeRowCodec, SqlInvalidation, committed_invalidations};
use futures::{FutureExt, executor::block_on};
use sql_translator::SqlTranslator;

#[test]
fn sql_rows_metadata_unknown_writes_and_derived_indexes_are_classified() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let engine = Engine::new(kernel.clone(), AutomergeRowCodec::new());
        let mut changes = engine.subscribe_changes().expect("supported");
        engine.translate_and_execute::<SqlTranslator>("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items (value) VALUES ('before')", &SqlTranslator).await.expect("seed");
        let initial = changes.select().expect("initial");
        assert_eq!(
            committed_invalidations(&engine, &initial)
                .await
                .expect("map"),
            vec![SqlInvalidation::All]
        );
        changes.acknowledge(initial);
        engine
            .translate_and_execute::<SqlTranslator>(
                "UPDATE items SET value = 'after'",
                &SqlTranslator,
            )
            .await
            .expect("update");
        let update = changes.select().expect("row update");
        let invalidations = committed_invalidations(&engine, &update)
            .await
            .expect("map");
        assert_eq!(invalidations.len(), 1);
        assert!(
            matches!(&invalidations[0], SqlInvalidation::Row { table, .. } if table == "items")
        );
        changes.acknowledge(update);
        engine
            .translate_and_execute::<SqlTranslator>("DELETE FROM items", &SqlTranslator)
            .await
            .expect("metadata delete");
        let delete = changes.select().expect("metadata changed");
        assert_eq!(
            committed_invalidations(&engine, &delete)
                .await
                .expect("map"),
            invalidations
        );
        changes.acknowledge(delete);
        let mut tx = kernel.transaction().await.expect("raw transaction");
        tx.put_bytes("items", vec![255], vec![1])
            .await
            .expect("unknown write");
        tx.commit().await.expect("commit");
        assert_eq!(
            committed_invalidations(&engine, &changes.select().expect("unknown invalidation"))
                .await
                .expect("map"),
            vec![SqlInvalidation::Table("items".into())]
        );
    });
}

#[test]
fn incoming_unique_key_cleanup_invalidates_both_requested_and_indirect_rows() {
    block_on(async {
        let source = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        let target = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        source
            .translate_and_execute(
                "CREATE TABLE items (id UUID PRIMARY KEY, value TEXT UNIQUE)",
                &SqlTranslator,
            )
            .await
            .expect("schema");
        sync::apply_sync_state_batch_for(
            &target,
            sync::export_sync_state_for(&source)
                .await
                .expect("export schema"),
        )
        .await
        .expect("schema apply");
        let low = "018f0f8e-7b6d-7c4a-8f12-000000000001";
        let high = "018f0f8e-7b6d-7c4a-8f12-000000000002";
        source
            .translate_and_execute(
                &format!("INSERT INTO items VALUES (CAST('{high}' AS UUID), 'same')"),
                &SqlTranslator,
            )
            .await
            .expect("winner");
        target
            .translate_and_execute(
                &format!("INSERT INTO items VALUES (CAST('{low}' AS UUID), 'same')"),
                &SqlTranslator,
            )
            .await
            .expect("loser");
        let mut changes = target.subscribe_changes().expect("supported");
        let mut other = changes.clone();
        changes.acknowledge(changes.select().expect("initial"));
        other.acknowledge(other.select().expect("initial other"));
        let incoming = sync::export_sync_state_for(&source)
            .await
            .expect("export winner");
        sync::apply_sync_state_batch_for(&target, incoming)
            .await
            .expect("incoming apply with cleanup");
        let selected = changes.select().expect("incoming fanout");
        let invalidations = committed_invalidations(&target, &selected)
            .await
            .expect("map");
        for id in [low, high] {
            assert!(
                invalidations.contains(&SqlInvalidation::Row {
                    table: "items".into(),
                    row: engine::Uuid::parse_str(id).expect("UUID")
                }),
                "requested winner and indirect loser: {invalidations:?}"
            );
        }
        changes.acknowledge(selected);
        assert!(other.select().is_some(), "independent downstream peer");
        source
            .translate_and_execute("DELETE FROM items", &SqlTranslator)
            .await
            .expect("metadata-only edit");
        let deleted = sync::export_sync_state_for(&source)
            .await
            .expect("export metadata");
        sync::apply_sync_state_batch_for(&target, deleted.clone())
            .await
            .expect("incoming metadata");
        let selected = changes.select().expect("metadata fanout");
        assert_eq!(
            committed_invalidations(&target, &selected)
                .await
                .expect("map"),
            vec![SqlInvalidation::Row {
                table: "items".into(),
                row: engine::Uuid::parse_str(high).expect("UUID")
            }]
        );
        changes.acknowledge(selected);
        sync::apply_sync_state_batch_for(&target, deleted)
            .await
            .expect("known state");
        assert!(
            changes.select().is_none(),
            "known history and metadata settle without echo"
        );
        source
            .translate_and_execute("DROP TABLE items", &SqlTranslator)
            .await
            .expect("drop");
        sync::apply_sync_state_batch_for(
            &target,
            sync::export_sync_state_for(&source)
                .await
                .expect("export drop"),
        )
        .await
        .expect("incoming drop");
        assert_eq!(
            committed_invalidations(&target, &changes.select().expect("catalog fanout"))
                .await
                .expect("map"),
            vec![SqlInvalidation::All]
        );
    });
}

#[test]
fn failed_engine_transaction_and_cancellation_never_lose_committed_work() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new());
        engine.translate_and_execute("CREATE TABLE items (id UUID PRIMARY KEY, value TEXT); INSERT INTO items (value) VALUES ('before')", &SqlTranslator).await.expect("seed");
        let mut changes = engine.subscribe_changes().expect("supported");
        changes.acknowledge(changes.select().expect("initial"));
        let mut tx = engine.transaction().await.expect("transaction");
        tx.translate_and_execute("UPDATE items SET value = 'discard'", &SqlTranslator)
            .await
            .expect("staged edit");
        assert!(
            tx.translate_and_execute("INSERT INTO missing VALUES (1)", &SqlTranslator)
                .await
                .is_err()
        );
        assert!(tx.commit().await.is_err());
        assert!(changes.select().is_none());
        let mut tx = engine.transaction().await.expect("transaction");
        tx.translate_and_execute("UPDATE items SET value = 'committed'", &SqlTranslator)
            .await
            .expect("staged edit");
        let caller = async move {
            tx.commit().await.expect("commit");
            std::future::pending::<()>().await;
        };
        assert!(
            caller.now_or_never().is_none(),
            "cancel caller after commit"
        );
        let selected = changes.select().expect("committed work retained");
        assert!(
            matches!(committed_invalidations(&engine, &selected).await.expect("map").as_slice(), [SqlInvalidation::Row { table, .. }] if table == "items")
        );
        drop(selected);
        assert!(
            changes.select().is_some(),
            "cancelled reconciliation retries without another write"
        );
    });
}

#[test]
fn memory_conflicts_and_rollback_do_not_publish() {
    block_on(async {
        let kernel = InMemoryKernel::new();
        let mut changes = kernel.subscribe_changes();
        changes.acknowledge(changes.select().expect("initial work"));
        let mut first = kernel.transaction().await.expect("first");
        let mut loser = kernel.transaction().await.expect("loser");
        first.ensure_table("items").await.expect("table");
        first
            .put_bytes("items", vec![1], vec![2])
            .await
            .expect("write");
        loser.ensure_table("lost").await.expect("table");
        first.commit().await.expect("commit");
        changes.acknowledge(changes.select().expect("winner"));
        assert!(loser.commit().await.is_err());
        assert!(changes.select().is_none());
        let mut tx = kernel.transaction().await.expect("transaction");
        tx.put_bytes("items", vec![1], vec![3])
            .await
            .expect("write");
        tx.rollback().await.expect("rollback");
        assert!(changes.select().is_none());
        kernel
            .transaction()
            .await
            .expect("read only")
            .commit()
            .await
            .expect("commit");
        assert!(changes.select().is_none());
    });
}
