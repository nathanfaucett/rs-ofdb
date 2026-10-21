use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

type Key = (String, Vec<u8>);
type Values = (Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Default)]
pub(crate) struct Mutations {
    keys: Mutex<BTreeMap<Key, Values>>,
    tables: Mutex<BTreeMap<String, (bool, bool)>>,
    dropped: AtomicBool,
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
            self.dropped.store(true, Ordering::Relaxed);
        }
        let mut tables = self.tables.lock().expect("transaction table mutation lock");
        let state = tables.entry(table.to_owned()).or_insert((old, old));
        state.1 = new;
    }

    pub(crate) fn changed(&self) -> bool {
        self.dropped.load(Ordering::Relaxed)
            || self
                .tables
                .lock()
                .expect("transaction table mutation lock")
                .values()
                .any(|(old, new)| old != new)
            || self
                .keys
                .lock()
                .expect("transaction mutation lock")
                .values()
                .any(|(old, new)| old != new)
    }
}
