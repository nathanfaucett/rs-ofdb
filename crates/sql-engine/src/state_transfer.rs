use alloc::{boxed::Box, collections::BTreeSet, string::String, vec::Vec};
use core::{future::Future, pin::Pin};

use value::Row;

use crate::{
    Engine, EngineResult, Kernel, KernelTransaction, RowCodec, Uuid,
    schema::{active_table_names, catalog_table_for_storage, ensure, reconcile_schema_names},
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MutationSummary {
    changed_tables: BTreeSet<String>,
    catalog_changed: bool,
}

impl MutationSummary {
    pub fn record_table(&mut self, table: &str) {
        if catalog_table_for_storage(table).is_some() {
            self.catalog_changed = true;
        } else {
            self.changed_tables.insert(String::from(table));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMutation {
    pub table: String,
    pub row: Uuid,
    pub old: Option<Row>,
    pub new: Option<Row>,
}

impl<K, R> Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn read_transaction<F, O>(&self, operation: F) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a K::Transaction,
        ) -> Pin<Box<dyn Future<Output = EngineResult<O>> + Send + 'a>>,
    {
        let mut transaction = self.kernel.transaction().await?;
        ensure(&mut transaction, self.reconciler.as_ref()).await?;
        let result = operation(self.reconciler.as_ref(), &transaction).await;
        transaction.rollback().await?;
        result
    }

    pub async fn mutate_transaction<F, O>(
        &self,
        table: &str,
        row: Uuid,
        operation: F,
    ) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
            Option<Row>,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, Option<Row>)>> + Send + 'a>,
        >,
    {
        crate::schema::validate_row_id(&row)?;
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            ensure(&mut transaction, self.reconciler.as_ref()).await?;
            self.reconciler
                .ensure_table(&mut transaction, table)
                .await?;
            let old = self.reconciler.get_row(&transaction, table, &row).await?;
            let (output, new) =
                operation(self.reconciler.as_ref(), &mut transaction, old.clone()).await?;
            if catalog_table_for_storage(table).is_none()
                && crate::schema::active_user_row(
                    &transaction,
                    self.reconciler.as_ref(),
                    table,
                    &row,
                )
                .await?
            {
                crate::index::update_row(
                    &mut transaction,
                    self.reconciler.as_ref(),
                    table,
                    old.as_ref(),
                    new.as_ref(),
                    false,
                )
                .await?;
                crate::index::rebuild_table(
                    &mut transaction,
                    self.reconciler.as_ref(),
                    table,
                    false,
                )
                .await?;
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                transaction.commit().await?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn mutate_tables<F, O>(&self, tables: &[String], operation: F) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, MutationSummary)>> + Send + 'a>,
        >,
    {
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            ensure(&mut transaction, self.reconciler.as_ref()).await?;
            for table in tables {
                self.reconciler
                    .ensure_table(&mut transaction, table)
                    .await?;
            }
            let (output, summary) = operation(self.reconciler.as_ref(), &mut transaction).await?;
            if summary.catalog_changed || !summary.changed_tables.is_empty() {
                reconcile_schema_names(&mut transaction, self.reconciler.as_ref()).await?;
            }
            let active_tables = active_table_names(&transaction, self.reconciler.as_ref()).await?;
            for table in active_tables {
                if summary.catalog_changed || summary.changed_tables.contains(&table) {
                    self.reconciler
                        .ensure_table(&mut transaction, &table)
                        .await?;
                    crate::index::rebuild_table(
                        &mut transaction,
                        self.reconciler.as_ref(),
                        &table,
                        false,
                    )
                    .await?;
                }
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                transaction.commit().await?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn mutate_rows<F, O>(&self, rows: &[(String, Uuid)], operation: F) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, Vec<RowMutation>)>> + Send + 'a>,
        >,
    {
        for (_, row) in rows {
            crate::schema::validate_row_id(row)?;
        }
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            ensure(&mut transaction, self.reconciler.as_ref()).await?;
            for (table, _) in rows {
                self.reconciler
                    .ensure_table(&mut transaction, table)
                    .await?;
            }
            let (output, mutations) = operation(self.reconciler.as_ref(), &mut transaction).await?;
            for mutation in &mutations {
                crate::schema::validate_row_id(&mutation.row)?;
            }
            let catalog_changed = mutations
                .iter()
                .any(|mutation| catalog_table_for_storage(&mutation.table).is_some());
            if catalog_changed {
                reconcile_schema_names(&mut transaction, self.reconciler.as_ref()).await?;
            }
            for mutation in mutations {
                if catalog_table_for_storage(&mutation.table).is_none()
                    && crate::schema::active_user_row(
                        &transaction,
                        self.reconciler.as_ref(),
                        &mutation.table,
                        &mutation.row,
                    )
                    .await?
                {
                    crate::index::update_row(
                        &mut transaction,
                        self.reconciler.as_ref(),
                        &mutation.table,
                        mutation.old.as_ref(),
                        mutation.new.as_ref(),
                        false,
                    )
                    .await?;
                }
            }
            let tables = if catalog_changed {
                active_table_names(&transaction, self.reconciler.as_ref()).await?
            } else {
                rows.iter()
                    .map(|(table, _)| table.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            for table in tables {
                if catalog_changed {
                    self.reconciler
                        .ensure_table(&mut transaction, &table)
                        .await?;
                    crate::index::rebuild_table(
                        &mut transaction,
                        self.reconciler.as_ref(),
                        &table,
                        false,
                    )
                    .await?;
                    continue;
                }
                let Some((_, row)) = rows.iter().find(|(row_table, _)| row_table == &table) else {
                    continue;
                };
                if crate::schema::active_user_row(
                    &transaction,
                    self.reconciler.as_ref(),
                    &table,
                    row,
                )
                .await?
                {
                    self.reconciler
                        .ensure_table(&mut transaction, &table)
                        .await?;
                    crate::index::rebuild_table(
                        &mut transaction,
                        self.reconciler.as_ref(),
                        &table,
                        false,
                    )
                    .await?;
                }
            }
            Ok(output)
        }
        .await;
        match result {
            Ok(output) => {
                transaction.commit().await?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn table_names(&self) -> EngineResult<Vec<String>> {
        let mut transaction = self.kernel.transaction().await?;
        ensure(&mut transaction, self.reconciler.as_ref()).await?;
        let result = active_table_names(&transaction, self.reconciler.as_ref()).await;
        transaction.rollback().await?;
        result
    }
}
