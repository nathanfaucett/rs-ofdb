use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_TABLE_FIELDS_STORAGE, Engine, EngineError, EngineResult,
    InMemoryKernel, Kernel, KernelTransaction, RowCatalog, RowCodec,
};
use engine_automerge::AutomergeRowCodec;
use futures::{Stream, StreamExt, executor::block_on, pin_mut};
use sql_translator::SqlTranslator;
use uuid::Uuid;
use value::{Row, Value};

#[derive(Default)]
struct FieldReads {
    scans: AtomicUsize,
    points: AtomicUsize,
    rows: AtomicUsize,
}

struct CountingCodec {
    inner: AutomergeRowCodec,
    reads: Arc<FieldReads>,
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

    async fn get_row(&self, transaction: &T, table: &str, row: &Uuid) -> EngineResult<Option<Row>> {
        let value = self.inner.get_row(transaction, table, row).await?;
        if table == ENGINE_TABLE_FIELDS_STORAGE {
            self.reads.points.fetch_add(1, Ordering::Relaxed);
            if value.is_some() {
                self.reads.rows.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(value)
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
        if table == ENGINE_TABLE_FIELDS_STORAGE {
            self.reads.scans.fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .scan_row_states(transaction, table)
            .inspect(move |value| {
                if table == ENGINE_TABLE_FIELDS_STORAGE && value.is_ok() {
                    self.reads.rows.fetch_add(1, Ordering::Relaxed);
                }
            })
    }
}

async fn fixture() -> (
    Engine<InMemoryKernel, CountingCodec>,
    InMemoryKernel,
    Arc<FieldReads>,
) {
    let kernel = InMemoryKernel::new();
    let reads = Arc::new(FieldReads::default());
    let database = Engine::new(
        kernel.clone(),
        CountingCodec {
            inner: AutomergeRowCodec::new(),
            reads: reads.clone(),
        },
    );
    database
        .translate_and_execute(
            "CREATE TABLE unrelated (id UUID PRIMARY KEY, a TEXT, b TEXT, c TEXT); \
             CREATE TABLE items (id UUID PRIMARY KEY, label TEXT, amount INTEGER); \
             CREATE UNIQUE INDEX items_order ON items (amount, label)",
            &SqlTranslator,
        )
        .await
        .expect("real catalog with unrelated fields and a composite index");
    (database, kernel, reads)
}

#[test]
fn index_schema_reads_known_field_ids_without_rescanning_the_catalog() {
    block_on(async {
        let (database, _, reads) = fixture().await;
        reads.scans.store(0, Ordering::Relaxed);
        reads.points.store(0, Ordering::Relaxed);
        reads.rows.store(0, Ordering::Relaxed);
        let index = database
            .index_schema("items_order")
            .await
            .expect("live index");
        assert_eq!(index.name, "items_order");
        assert_eq!(index.table_name, "items");
        assert_eq!(index.column_indices, vec![2, 1]);
        assert!(index.unique);
        let actual = (
            reads.scans.load(Ordering::Relaxed),
            reads.points.load(Ordering::Relaxed),
            reads.rows.load(Ordering::Relaxed),
        );
        assert_eq!(
            actual,
            (1, 2, 9),
            "one complete seven-field schema validation plus two exact field reads"
        );
    });
}

#[test]
fn index_schema_rechecks_exact_field_identity_and_complete_current_catalog() {
    block_on(async {
        for mutation in [
            "missing",
            "deleted",
            "table",
            "name",
            "malformed",
            "unrelated",
        ] {
            let (database, kernel, _) = fixture().await;
            database
                .index_schema("items_order")
                .await
                .expect("valid earlier snapshot");
            let codec = AutomergeRowCodec::new();
            let mut transaction = kernel.transaction().await.expect("catalog mutation");
            let (field_id, mut field) = {
                let rows = codec.scan_row_states(&transaction, ENGINE_TABLE_FIELDS_STORAGE);
                pin_mut!(rows);
                let mut selected = None;
                while let Some(row) = rows.next().await {
                    let (id, value, _) = row.expect("catalog field");
                    if value.values[1] == Value::from("items")
                        && value.values[2] == Value::from("amount")
                    {
                        selected = Some((id, value));
                    }
                }
                selected.expect("exact indexed field fixture")
            };
            match mutation {
                "missing" => {
                    let (id, mut reference) = {
                        let rows = codec.scan_row_states(&transaction, ENGINE_INDEX_FIELDS_STORAGE);
                        pin_mut!(rows);
                        let mut selected = None;
                        while let Some(row) = rows.next().await {
                            let (id, value, _) = row.expect("index reference");
                            if value.values[3] == Value::Uuid(field_id) {
                                selected = Some((id, value));
                            }
                        }
                        selected.expect("known UUID reference")
                    };
                    reference.values[3] = Value::Uuid(Uuid::now_v7());
                    codec
                        .put_row(&mut transaction, ENGINE_INDEX_FIELDS_STORAGE, id, reference)
                        .await
                        .expect("missing exact reference");
                }
                "deleted" => {
                    codec
                        .delete_row(&mut transaction, ENGINE_TABLE_FIELDS_STORAGE, &field_id)
                        .await
                        .expect("tombstone exact field");
                }
                "table" | "name" => {
                    field.values[if mutation == "table" { 1 } else { 2 }] = Value::from("changed");
                    codec
                        .put_row(
                            &mut transaction,
                            ENGINE_TABLE_FIELDS_STORAGE,
                            field_id,
                            field,
                        )
                        .await
                        .expect("mismatched exact field");
                }
                "malformed" | "unrelated" => {
                    let id = if mutation == "malformed" {
                        field_id
                    } else {
                        Uuid::now_v7()
                    };
                    codec
                        .put_row(
                            &mut transaction,
                            ENGINE_TABLE_FIELDS_STORAGE,
                            id,
                            Row::new(vec![Value::Integer(42)]),
                        )
                        .await
                        .expect("malformed catalog field");
                }
                _ => unreachable!("fixed mutation cases"),
            }
            transaction
                .commit()
                .await
                .expect("commit later catalog snapshot");
            let error = database
                .index_schema("items_order")
                .await
                .expect_err("fresh index schema must reject changed field evidence");
            if ["malformed", "unrelated"].contains(&mutation) {
                assert!(
                    matches!(error, EngineError::Custom(message) if message == "Invalid column table identity"),
                    "{mutation}"
                );
            } else {
                assert!(
                    matches!(
                        error,
                        EngineError::InvalidQuery("Index column identity is inactive")
                    ),
                    "{mutation}"
                );
            }
        }
    });
}
