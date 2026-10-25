use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
    ENGINE_TABLES_STORAGE, Engine, EngineError, EngineResult, Kernel, Uuid,
    catalog_table_for_storage,
};

use value::{Row, Value, ValueType};

use crate::candidate::CandidateValidator;
use crate::{SyncCandidateView, SyncDirection, SyncKey, SyncRowCodec, SyncStateUnit};

type Columns = Vec<(String, ValueType, Value, bool)>;
type Index = (String, bool, Vec<String>);

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
type ValueValidator<'a> = dyn Fn(&str, &str, &Value) -> Result<(), String> + Send + Sync + 'a;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type ValueValidator<'a> = dyn Fn(&str, &str, &Value) -> Result<(), String> + 'a;

#[derive(Clone)]
pub(crate) struct TableScope<'a> {
    validate_value: &'a ValueValidator<'a>,
    pub validate_candidate: Option<&'a CandidateValidator<'a>>,
    pub direction: SyncDirection,
    pub names: Vec<String>,
    columns: BTreeMap<String, Columns>,
    indices: BTreeMap<String, Index>,
    tables: BTreeMap<Uuid, String>,
    fields: BTreeMap<Uuid, (String, String)>,
    index_ids: BTreeMap<Uuid, String>,
    pub keys: BTreeSet<SyncKey>,
}

fn rejected() -> EngineError {
    EngineError::InvalidQuery("History is outside fixed table scope or local schema")
}
fn text(row: &Row, index: usize) -> EngineResult<String> {
    row.values
        .get(index)
        .and_then(Value::to_text)
        .ok_or_else(rejected)
}
fn uuid(row: &Row, index: usize) -> EngineResult<Uuid> {
    row.values
        .get(index)
        .and_then(Value::to_uuid)
        .ok_or_else(rejected)
}

impl<'a> TableScope<'a> {
    pub async fn new<K, R>(
        engine: &Engine<K, R>,
        names: &[&str],
        validate_value: &'a ValueValidator<'a>,
    ) -> EngineResult<Self>
    where
        K: Kernel,
        R: SyncRowCodec<K::Transaction>,
    {
        let mut scope = Self {
            validate_value,
            validate_candidate: None,
            direction: SyncDirection::Bidirectional,
            names: Vec::new(),
            columns: BTreeMap::new(),
            indices: BTreeMap::new(),
            tables: BTreeMap::new(),
            fields: BTreeMap::new(),
            index_ids: BTreeMap::new(),
            keys: BTreeSet::new(),
        };
        if names.is_empty() {
            return Err(rejected());
        }
        let mut unique = BTreeSet::new();
        for name in names {
            if name.is_empty() || catalog_table_for_storage(name).is_some() || !unique.insert(*name)
            {
                return Err(rejected());
            }
        }
        let (schemas, indices, rows) = engine.observe_schema(names, None).await?;
        for (name, schema) in names.iter().zip(schemas) {
            scope.columns.insert(
                name.to_string(),
                schema
                    .columns
                    .into_iter()
                    .map(|c| (c.name, c.r#type, c.default, c.primary_key))
                    .collect(),
            );
            for (column, _, default, _) in &scope.columns[*name] {
                (validate_value)(name, column, default).map_err(EngineError::custom)?;
            }
            scope.names.push(name.to_string());
        }
        scope.names.sort();

        for (table, id, row) in &rows {
            let selected = match *table {
                ENGINE_TABLES_STORAGE => scope.columns.contains_key(&text(row, 0)?),
                ENGINE_TABLE_FIELDS_STORAGE | ENGINE_INDICES_STORAGE => {
                    scope.columns.contains_key(&text(row, 1)?)
                }
                ENGINE_INDEX_FIELDS_STORAGE => scope.index_ids.contains_key(&uuid(row, 0)?),
                _ => false,
            };
            if !selected {
                continue;
            }
            if *table == ENGINE_INDICES_STORAGE {
                let name = text(row, 0)?;
                let index = indices
                    .iter()
                    .find(|index| index.name == name)
                    .ok_or_else(rejected)?;
                let columns = scope.columns.get(&index.table_name).ok_or_else(rejected)?;
                let fields = index
                    .column_indices
                    .iter()
                    .map(|i| {
                        columns
                            .get(*i as usize)
                            .map(|c| c.0.clone())
                            .ok_or_else(rejected)
                    })
                    .collect::<EngineResult<Vec<_>>>()?;
                scope
                    .indices
                    .insert(name, (index.table_name.clone(), index.unique, fields));
            }
            scope.admit_catalog(table, *id, row)?;
        }
        Ok(scope)
    }

    pub fn permits(&self, key: &SyncKey) -> bool {
        let SyncKey::Row { table, .. } = key;
        self.columns.contains_key(table) || self.keys.contains(key)
    }

    pub fn permits_address(&self, table: &str) -> bool {
        self.columns.contains_key(table) || catalog_table_for_storage(table).is_some()
    }

    fn admit_catalog(&mut self, table: &str, id: Uuid, row: &Row) -> EngineResult<()> {
        match table {
            ENGINE_TABLES_STORAGE => {
                let name = text(row, 0)?;
                if !self.columns.contains_key(&name) {
                    return Err(rejected());
                }
                self.tables.insert(id, name);
            }
            ENGINE_TABLE_FIELDS_STORAGE => {
                let table = text(row, 1)?;
                if self.tables.get(&uuid(row, 0)?) != Some(&table) {
                    return Err(rejected());
                }
                let name = text(row, 2)?;
                let columns = self.columns.get(&table).ok_or_else(rejected)?;
                let column = columns.iter().find(|c| c.0 == name).ok_or_else(rejected)?;
                if row.values.get(3).and_then(Value::to_type) != Some(column.1)
                    || row.values.get(4) != Some(&column.2)
                    || row.values.get(6).and_then(Value::to_bool) != Some(column.3)
                    || row
                        .values
                        .get(5)
                        .and_then(Value::to_integer)
                        .is_none_or(|p| p < 0 || p as usize >= columns.len())
                {
                    return Err(rejected());
                }
                (self.validate_value)(&table, &name, &column.2).map_err(EngineError::custom)?;
                self.fields.insert(id, (table, name));
            }
            ENGINE_INDICES_STORAGE => {
                let name = text(row, 0)?;
                let table = text(row, 1)?;
                let index = self.indices.get(&name).ok_or_else(rejected)?;
                if index.0 != table
                    || row.values.get(2).and_then(Value::to_bool) != Some(index.1)
                    || self.tables.get(&uuid(row, 3)?) != Some(&table)
                {
                    return Err(rejected());
                }
                self.index_ids.insert(id, name);
            }
            ENGINE_INDEX_FIELDS_STORAGE => {
                let name = self.index_ids.get(&uuid(row, 0)?).ok_or_else(rejected)?;
                let index = self.indices.get(name).ok_or_else(rejected)?;
                let position = row
                    .values
                    .get(1)
                    .and_then(Value::to_integer)
                    .ok_or_else(rejected)?;
                let column = text(row, 2)?;
                if position < 0
                    || index.2.get(position as usize) != Some(&column)
                    || self.fields.get(&uuid(row, 3)?) != Some(&(index.0.clone(), column))
                {
                    return Err(rejected());
                }
            }
            _ => return Err(rejected()),
        }
        self.keys.insert(SyncKey::Row {
            table: table.to_string(),
            row: id,
        });
        Ok(())
    }

    pub async fn validate<K, R>(
        &mut self,
        engine: &Engine<K, R>,
        unit: &SyncStateUnit,
    ) -> EngineResult<()>
    where
        K: Kernel,
        R: SyncRowCodec<K::Transaction>,
    {
        let SyncKey::Row { table, row } = &unit.key;
        if !self.permits_address(table) {
            return Err(rejected());
        }
        let columns = if let Some(catalog) = catalog_table_for_storage(table) {
            catalog
                .columns()
                .iter()
                .map(|name| (name.to_string(), ValueType::Null, Value::Null, false))
                .collect::<Vec<_>>()
        } else {
            self.columns
                .get(table)
                .ok_or_else(rejected)?
                .iter()
                .map(|c| (c.0.clone(), c.1, c.2.clone(), c.3))
                .collect()
        };
        let table_name = table.clone();
        let id = *row;
        let payload = unit.state.clone();
        let metadata = unit.metadata.clone();
        let (value, schemas, observations) = engine
            .read_transaction(move |codec, tx| {
                Box::pin(async move {
                    codec
                        .inspect_history(tx, &table_name, id, &payload, &metadata, &columns)
                        .await
                })
            })
            .await?;
        if catalog_table_for_storage(table).is_some() {
            self.admit_catalog(table, *row, &value)?;
        } else {
            if schemas.iter().any(|id| self.tables.get(id) != Some(table)) {
                return Err(rejected());
            }
            for (column, value) in observations {
                (self.validate_value)(table, &column, &value).map_err(EngineError::custom)?;
            }
        }
        Ok(())
    }

    pub fn candidate_tables(&self) -> Option<Vec<(String, Vec<String>)>> {
        self.validate_candidate.map(|_| {
            self.names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        self.columns[name]
                            .iter()
                            .map(|column| column.0.clone())
                            .collect(),
                    )
                })
                .collect()
        })
    }

    pub fn validate_candidates(&self, view: &Option<SyncCandidateView>) -> EngineResult<()> {
        if let (Some(validate), Some(view)) = (self.validate_candidate, view) {
            validate(view).map_err(EngineError::custom)?;
        }
        Ok(())
    }

    pub async fn unchanged<K, R>(&self, engine: &Engine<K, R>) -> EngineResult<()>
    where
        K: Kernel,
        R: SyncRowCodec<K::Transaction>,
    {
        let names = self.columns.keys().map(String::as_str).collect::<Vec<_>>();
        let index_names = self.indices.keys().map(String::as_str).collect::<Vec<_>>();
        let (tables, indices, _) = engine.observe_schema(&names, Some(&index_names)).await?;
        for ((_, expected), current) in self.columns.iter().zip(&tables) {
            let mut actual = current
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.r#type, c.default.clone(), c.primary_key))
                .collect::<Vec<_>>();
            let mut expected = expected.clone();
            actual.sort_by(|a, b| a.0.cmp(&b.0));
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            if actual != expected {
                return Err(rejected());
            }
        }
        for ((_, (table, unique, fields)), current) in self.indices.iter().zip(indices) {
            let columns = &tables
                .iter()
                .find(|schema| schema.name == *table)
                .ok_or_else(rejected)?
                .columns;
            let actual = current
                .column_indices
                .iter()
                .map(|i| {
                    columns
                        .get(*i as usize)
                        .map(|c| c.name.clone())
                        .ok_or_else(rejected)
                })
                .collect::<EngineResult<Vec<_>>>()?;
            if current.table_name != *table || current.unique != *unique || actual != *fields {
                return Err(rejected());
            }
        }
        Ok(())
    }
}
