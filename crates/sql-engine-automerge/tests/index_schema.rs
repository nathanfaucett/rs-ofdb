use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE, Engine,
    EngineError, EngineResult, InMemoryKernel, Kernel, KernelTransaction, RowCatalog, RowCodec,
};
use engine_automerge::AutomergeRowCodec;
use futures::{Stream, StreamExt, executor::block_on, pin_mut};
use query::{Query, QueryInsert, Statement};
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
fn insert_reuses_current_table_schema_for_each_additional_index() {
    block_on(async {
        let mut costs = Vec::new();
        for indexed in [false, true] {
            let (database, _, reads) = fixture().await;
            if indexed {
                database
                    .translate_and_execute(
                        "CREATE INDEX items_label ON items (label); \
                         CREATE INDEX items_amount ON items (amount); \
                         CREATE INDEX items_label_amount ON items (label, amount)",
                        &SqlTranslator,
                    )
                    .await
                    .expect("three additional non-unique indexes");
            }
            let row = Row::new(vec![
                Value::Uuid(Uuid::now_v7()),
                Value::from("inserted"),
                Value::Integer(7),
            ]);
            reads.scans.store(0, Ordering::Relaxed);
            database
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "items".into(),
                    row: row.clone(),
                    returning: None,
                }))])
                .await
                .expect("public INSERT maintains current indexes");
            let scans = reads.scans.load(Ordering::Relaxed);
            assert!(
                scans > 0,
                "INSERT still validates the complete current catalog"
            );
            costs.push(scans);
            let mut lookups = vec![(
                "items_order",
                Row::new(vec![Value::Integer(7), Value::from("inserted")]),
            )];
            if indexed {
                lookups.extend([
                    ("items_label", Row::new(vec![Value::from("inserted")])),
                    ("items_amount", Row::new(vec![Value::Integer(7)])),
                    (
                        "items_label_amount",
                        Row::new(vec![Value::from("inserted"), Value::Integer(7)]),
                    ),
                ]);
            }
            for (name, key) in lookups {
                assert_eq!(
                    database
                        .index_lookup(name, &key)
                        .await
                        .expect("current index lookup"),
                    Some(row.clone()),
                    "{name} points to the inserted row with the correct field order"
                );
            }
        }
        assert_eq!(
            costs[1] - costs[0],
            0,
            "three additional indexes must not reconstruct the already-validated field catalog: {costs:?}"
        );
    });
}

#[test]
fn index_updates_use_fresh_schema_after_ddl_in_the_same_batch() {
    block_on(async {
        let (database, _, _) = fixture().await;
        let initial = Uuid::now_v7();
        database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "items".into(),
                row: Row::new(vec![
                    Value::Uuid(initial),
                    Value::from("before"),
                    Value::Integer(1),
                ]),
                returning: None,
            }))])
            .await
            .expect("earlier operation uses its current schema");
        let later = Uuid::now_v7();
        database
            .translate_and_execute(
                &format!(
                    "ALTER TABLE items ADD COLUMN fresh TEXT DEFAULT 'current'; \
                     CREATE INDEX items_fresh ON items (fresh, label); \
                     INSERT INTO items VALUES (CAST('{later}' AS UUID), 'after', 2, 'later')"
                ),
                &SqlTranslator,
            )
            .await
            .expect("mixed DDL/write batch observes each operation's current schema");
        for (id, label, amount, fresh) in [
            (initial, "before", 1, "current"),
            (later, "after", 2, "later"),
        ] {
            assert_eq!(
                database
                    .index_lookup(
                        "items_fresh",
                        &Row::new(vec![Value::from(fresh), Value::from(label)])
                    )
                    .await
                    .expect("index uses fresh field positions and defaults"),
                Some(Row::new(vec![
                    Value::Uuid(id),
                    Value::from(label),
                    Value::Integer(amount),
                    Value::from(fresh)
                ]))
            );
        }
    });
}

#[test]
fn index_updates_validate_a_competing_winner_in_its_own_table() {
    block_on(async {
        let (database, kernel, _) = fixture().await;
        let codec = AutomergeRowCodec::new();
        let mut transaction = kernel.transaction().await.expect("competing index fixture");
        let (column_id, table_id) = {
            let fields = codec.scan_row_states(&transaction, ENGINE_TABLE_FIELDS_STORAGE);
            pin_mut!(fields);
            let mut selected = None;
            while let Some(field) = fields.next().await {
                let (id, row, _) = field.expect("live field");
                if row.values[1] == Value::from("unrelated") && row.values[2] == Value::from("a") {
                    selected = Some((id, row.values[0].clone()));
                }
            }
            selected.expect("winner's exact field identity")
        };
        let winner = Uuid::now_v7();
        codec
            .put_row(
                &mut transaction,
                ENGINE_INDICES_STORAGE,
                winner,
                Row::new(vec![
                    Value::from("items_order"),
                    Value::from("unrelated"),
                    Value::Bool(false),
                    table_id,
                ]),
            )
            .await
            .expect("later same-name index belongs to a different table");
        codec
            .put_row(
                &mut transaction,
                ENGINE_INDEX_FIELDS_STORAGE,
                Uuid::now_v7(),
                Row::new(vec![
                    Value::Uuid(winner),
                    Value::Integer(0),
                    Value::from("a"),
                    Value::Uuid(column_id),
                ]),
            )
            .await
            .expect("winner refers to a column absent from items");
        transaction
            .commit()
            .await
            .expect("commit competing definition");
        database
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "items".into(),
                row: Row::new(vec![
                    Value::Uuid(Uuid::now_v7()),
                    Value::from("inserted"),
                    Value::Integer(7),
                ]),
                returning: None,
            }))])
            .await
            .expect("items schema must not be reused to validate the other table's winner");
        let index = database
            .index_schema("items_order")
            .await
            .expect("current winning index");
        assert_eq!(index.table_name, "unrelated");
        assert_eq!(index.column_indices, vec![1]);
        assert!(!index.unique);
        let rows = database
            .translate_and_execute("SELECT * FROM items", &SqlTranslator)
            .await
            .expect("inserted item");
        assert_eq!(rows[0].rows.len(), 1);
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
            let mut values = vec![Value::Uuid(Uuid::now_v7()), Value::from("new row")];
            if !["deleted", "table"].contains(&mutation) {
                values.push(Value::Integer(7));
            }
            let insertion_error = database
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "items".into(),
                    row: Row::new(values),
                    returning: None,
                }))])
                .await
                .expect_err("INSERT must retain current field identity and full catalog checks");
            if mutation == "table" {
                assert!(matches!(
                    insertion_error,
                    EngineError::InvalidQuery("Row has the wrong column count")
                ));
            } else {
                assert_eq!(insertion_error.to_string(), error.to_string(), "{mutation}");
            }
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
