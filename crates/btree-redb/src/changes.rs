use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use btree::StorageInvalidation;

type Key = (String, Vec<u8>);
type Values = (Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Default)]
pub(crate) struct Mutations {
    keys: Mutex<BTreeMap<Key, Values>>,
    tables: Mutex<BTreeMap<String, (bool, bool)>>,
    dropped: Mutex<BTreeSet<String>>,
}

impl Mutations {
    pub(crate) fn table_exists(&self, table: &str) -> Option<bool> {
        self.tables
            .lock()
            .expect("transaction table mutation lock")
            .get(table)
            .map(|(_, current)| *current)
    }

    pub(crate) fn record(
        &self,
        table: &str,
        key: Vec<u8>,
        old: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    ) {
        let mut keys = self.keys.lock().expect("transaction mutation lock");
        let values = keys
            .entry((table.to_owned(), key))
            .or_insert_with(|| (old, None));
        values.1 = new;
    }

    pub(crate) fn table(&self, table: &str, old: bool, new: bool) {
        if old && !new {
            self.dropped
                .lock()
                .expect("transaction dropped table lock")
                .insert(table.to_owned());
        }
        let mut tables = self.tables.lock().expect("transaction table mutation lock");
        let state = tables.entry(table.to_owned()).or_insert((old, old));
        state.1 = new;
    }

    pub(crate) fn invalidations(&self) -> Vec<StorageInvalidation> {
        let dropped = self.dropped.lock().expect("transaction dropped table lock");
        let mut result = self
            .tables
            .lock()
            .expect("transaction table mutation lock")
            .iter()
            .filter(|(table, (old, new))| old != new || dropped.contains(*table))
            .map(|(table, _)| StorageInvalidation::Table(table.clone()))
            .collect::<Vec<_>>();
        result.extend(
            self.keys
                .lock()
                .expect("transaction mutation lock")
                .iter()
                .filter(|(_, (old, new))| old != new)
                .map(|((table, key), _)| StorageInvalidation::Key {
                    table: table.clone(),
                    key: key.clone(),
                }),
        );
        result
    }
}
