use crate::{
    Key, RedbKey, RedbValue, Value,
    read_budget::{Borrowed, ReadBudget},
};
use btree::{BTreeError, BTreeResult};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::{
    any::TypeId,
    collections::BTreeMap,
    ops::RangeBounds,
    sync::{Arc, Mutex},
};
const VERSIONS: TableDefinition<&str, u64> = TableDefinition::new("ofdb.shared.versions");
const RETIRED: TableDefinition<&str, bool> = TableDefinition::new("ofdb.shared.retired");
type Apply = fn(&redb::WriteTransaction, &str, &TableState) -> BTreeResult<()>;
struct TableState {
    types: (TypeId, TypeId),
    version: u64,
    dropped: bool,
    created: bool,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    apply: Apply,
}
pub struct SharedWriteTransaction {
    database: Arc<redb::Database>,
    snapshot: redb::ReadTransaction,
    tables: Mutex<BTreeMap<String, TableState>>,
}
impl SharedWriteTransaction {
    pub fn new(database: Arc<redb::Database>) -> BTreeResult<Self> {
        let snapshot = database.begin_read().map_err(BTreeError::custom)?;
        Ok(Self {
            database,
            snapshot,
            tables: Mutex::new(BTreeMap::new()),
        })
    }
    fn observe<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<()> {
        let mut tables = self.tables.lock().expect("shared transaction tables lock");
        if let Some(table) = tables.get(name) {
            return if table.types == (TypeId::of::<K>(), TypeId::of::<V>()) {
                Ok(())
            } else {
                Err(BTreeError::TypeMismatch)
            };
        }
        match self
            .snapshot
            .open_table(crate::table_definition::<K, V>(name))
        {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(BTreeError::custom(error)),
        }
        let version = match self.snapshot.open_table(VERSIONS) {
            Ok(table) => table
                .get(name)
                .map_err(BTreeError::custom)?
                .map_or(0, |value| value.value()),
            Err(redb::TableError::TableDoesNotExist(_)) => 0,
            Err(error) => return Err(BTreeError::custom(error)),
        };
        tables.insert(
            name.to_owned(),
            TableState {
                types: (TypeId::of::<K>(), TypeId::of::<V>()),
                version,
                dropped: false,
                created: false,
                writes: BTreeMap::new(),
                apply: apply::<K, V>,
            },
        );
        Ok(())
    }
    pub fn ensure_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<()> {
        self.observe::<K, V>(name)?;
        let mut tables = self.tables.lock().expect("shared transaction tables lock");
        let state = tables.get_mut(name).expect("observed table");
        if state.dropped
            || !self
                .snapshot
                .list_tables()
                .map_err(BTreeError::custom)?
                .any(|table| table.name() == name)
        {
            state.created = true;
        }
        Ok(())
    }
    pub fn drop_table<K: RedbKey, V: RedbValue>(&self, name: &str) -> BTreeResult<bool> {
        self.observe::<K, V>(name)?;
        let mut tables = self.tables.lock().expect("shared transaction tables lock");
        let table = tables.get_mut(name).expect("observed table");
        let existed = table.created
            || (!table.dropped
                && self
                    .snapshot
                    .list_tables()
                    .map_err(BTreeError::custom)?
                    .any(|table| table.name() == name));
        table.dropped = true;
        table.created = false;
        table.writes.clear();
        Ok(existed)
    }
    pub fn get<K: RedbKey, V: RedbValue>(&self, name: &str, key: &K) -> BTreeResult<Option<V>> {
        self.observe::<K, V>(name)?;
        {
            let tables = self.tables.lock().expect("shared transaction tables lock");
            let table = tables.get(name).expect("observed table");
            if let Some(value) = table.writes.get(K::as_bytes(key).as_ref()) {
                return Ok(value.as_ref().map(|bytes| V::from_bytes(bytes)));
            }
            if table.dropped {
                return Ok(None);
            }
        }
        match self
            .snapshot
            .open_table(TableDefinition::<Borrowed<Key<K>>, Borrowed<Value<V>>>::new(name))
        {
            Ok(table) => Ok(table
                .get(K::as_bytes(key).as_ref())
                .map_err(BTreeError::custom)?
                .map(|value| V::from_bytes(value.value()))),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(error) => Err(BTreeError::custom(error)),
        }
    }
    pub fn range<K: RedbKey, V: RedbValue, R: RangeBounds<K>>(
        &self,
        name: &str,
        range: R,
        budget: &ReadBudget,
    ) -> BTreeResult<Vec<(K, V)>> {
        self.observe::<K, V>(name)?;
        let tables = self.tables.lock().expect("shared transaction tables lock");
        let state = tables.get(name).expect("observed table");
        let mut entries = BTreeMap::new();
        if !state.dropped {
            match self
                .snapshot
                .open_table(TableDefinition::<Borrowed<Key<K>>, Borrowed<Value<V>>>::new(name))
            {
                Ok(table) => {
                    for entry in table.iter().map_err(BTreeError::custom)? {
                        let (key, value) = entry.map_err(BTreeError::custom)?;
                        let decoded = K::from_bytes(key.value());
                        if range.contains(&decoded) {
                            budget.charge(key.value().len().saturating_add(value.value().len()))?;
                            entries.insert(decoded, V::from_bytes(value.value()));
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(BTreeError::custom(error)),
            }
        }
        for (key, value) in &state.writes {
            let decoded = K::from_bytes(key);
            if range.contains(&decoded) {
                if let Some(value) = value {
                    budget.charge(key.len().saturating_add(value.len()))?;
                    entries.insert(decoded, V::from_bytes(value));
                } else {
                    entries.remove(&decoded);
                }
            }
        }
        Ok(entries.into_iter().collect())
    }
    pub fn put<K: RedbKey, V: RedbValue>(
        &self,
        name: &str,
        key: &K,
        value: Option<&V>,
    ) -> BTreeResult<()> {
        self.observe::<K, V>(name)?;
        let mut tables = self.tables.lock().expect("shared transaction tables lock");
        let state = tables.get_mut(name).expect("observed table");
        state.created = true;
        state.writes.insert(
            K::as_bytes(key).as_ref().to_vec(),
            value.map(|value| V::as_bytes(value).as_ref().to_vec()),
        );
        Ok(())
    }
    pub fn commit(self) -> BTreeResult<()> {
        let tables = self
            .tables
            .into_inner()
            .expect("shared transaction tables lock");
        if !tables
            .values()
            .any(|table| table.created || table.dropped || !table.writes.is_empty())
        {
            return Ok(());
        }
        let transaction = self.database.begin_write().map_err(BTreeError::custom)?;
        {
            let mut versions = transaction
                .open_table(VERSIONS)
                .map_err(BTreeError::custom)?;
            let retired = transaction
                .open_table(RETIRED)
                .map_err(BTreeError::custom)?;
            for (name, state) in &tables {
                let version = versions
                    .get(name.as_str())
                    .map_err(BTreeError::custom)?
                    .map_or(0, |value| value.value());
                if version != state.version {
                    return Err(BTreeError::Conflict);
                }
                if let Some(namespace) = namespace_prefix(name)
                    && retired
                        .get(namespace)
                        .map_err(BTreeError::custom)?
                        .is_some()
                {
                    return Err(BTreeError::custom(crate::resource::retired_error()));
                }
            }
            for (name, state) in &tables {
                if state.created || state.dropped || !state.writes.is_empty() {
                    (state.apply)(&transaction, name, state)?;
                    versions
                        .insert(
                            name.as_str(),
                            state.version.checked_add(1).ok_or(BTreeError::Conflict)?,
                        )
                        .map_err(BTreeError::custom)?;
                }
            }
        }
        transaction.commit().map_err(BTreeError::custom)
    }
}
fn namespace_prefix(name: &str) -> Option<&str> {
    let suffix = name.strip_prefix("ofdb.ns.")?;
    let (length, _) = suffix.split_once(':')?;
    let length: usize = length.parse().ok()?;
    let end = "ofdb.ns."
        .len()
        .checked_add(suffix.find(':')?)?
        .checked_add(1)?
        .checked_add(length)?
        .checked_add(1)?;
    name.get(..end)
}
fn apply<K: RedbKey, V: RedbValue>(
    transaction: &redb::WriteTransaction,
    name: &str,
    state: &TableState,
) -> BTreeResult<()> {
    let definition = crate::table_definition::<K, V>(name);
    if state.dropped {
        transaction
            .delete_table(definition)
            .map_err(BTreeError::custom)?;
    }
    if state.created {
        let mut table = transaction
            .open_table(definition)
            .map_err(BTreeError::custom)?;
        for (key, value) in &state.writes {
            let key = Key::new(K::from_bytes(key));
            if let Some(value) = value {
                table
                    .insert(key, Value::new(V::from_bytes(value)))
                    .map_err(BTreeError::custom)?;
            } else {
                table.remove(key).map_err(BTreeError::custom)?;
            }
        }
    }
    Ok(())
}
