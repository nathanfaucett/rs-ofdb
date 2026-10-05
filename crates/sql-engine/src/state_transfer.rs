use alloc::{boxed::Box, collections::BTreeSet, string::String, vec::Vec};
use core::{future::Future, pin::Pin};

use value::Row;

use crate::{
    Engine, EngineResult, Kernel, KernelTransaction, RowCodec, RowIdentity,
    schema::{active_table_names, catalog_table_for_storage, ensure},
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
    pub row: RowIdentity,
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
        row: RowIdentity,
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

    pub async fn mutate_rows<F, O>(
        &self,
        rows: &[(String, RowIdentity)],
        operation: F,
    ) -> EngineResult<O>
    where
        F: for<'a> FnOnce(
            &'a R,
            &'a mut K::Transaction,
        ) -> Pin<
            Box<dyn Future<Output = EngineResult<(O, Vec<RowMutation>)>> + Send + 'a>,
        >,
    {
        let mut transaction = self.kernel.transaction().await?;
        let result: EngineResult<O> = async {
            ensure(&mut transaction, self.reconciler.as_ref()).await?;
            for (table, _) in rows {
                self.reconciler
                    .ensure_table(&mut transaction, table)
                    .await?;
            }
            let (output, mutations) = operation(self.reconciler.as_ref(), &mut transaction).await?;
            let catalog_changed = mutations
                .iter()
                .any(|mutation| catalog_table_for_storage(&mutation.table).is_some());
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
            if catalog_changed {
                for table in active_table_names(&transaction, self.reconciler.as_ref()).await? {
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
