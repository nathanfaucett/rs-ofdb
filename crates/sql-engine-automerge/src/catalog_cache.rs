use std::{collections::BTreeMap, sync::Mutex};

use btree_automerge::{DocumentChangeKey, DocumentId};
use uuid::Uuid;
use value::Row;

const MAX_ENTRIES: usize = 2048;
const MAX_BYTES: usize = 8 * 1024 * 1024;

type History = Vec<(DocumentChangeKey, Vec<u8>)>;

#[derive(Debug)]
struct Entry {
    history: History,
    row: Row,
    bytes: usize,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<(String, DocumentId), Entry>,
    bytes: usize,
}

#[derive(Debug, Default)]
pub struct CatalogCache(Mutex<State>);

#[derive(Debug)]
struct Snapshot {
    source: Vec<(Vec<u8>, Vec<u8>)>,
    rows: Vec<(Uuid, Row, bool)>,
    bytes: usize,
}

#[derive(Debug, Default)]
pub struct CatalogSnapshots(Mutex<BTreeMap<String, Snapshot>>);

impl CatalogSnapshots {
    pub fn get(
        &self,
        table: &str,
        source: &[(Vec<u8>, Vec<u8>)],
    ) -> Option<Vec<(Uuid, Row, bool)>> {
        self.0
            .lock()
            .expect("catalog snapshot cache lock")
            .get(table)
            .filter(|snapshot| snapshot.source == source)
            .map(|snapshot| snapshot.rows.clone())
    }

    pub fn insert(
        &self,
        table: &str,
        source: Vec<(Vec<u8>, Vec<u8>)>,
        rows: Vec<(Uuid, Row, bool)>,
    ) {
        let Ok(encoded) = postcard::to_allocvec(&rows) else {
            return;
        };
        let bytes = source
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
            + encoded.len()
            + rows.len() * size_of::<(Uuid, Row, bool)>();
        if bytes > MAX_BYTES {
            return;
        }
        let mut snapshots = self.0.lock().expect("catalog snapshot cache lock");
        snapshots.remove(table);
        if snapshots.len() >= 4
            || snapshots
                .values()
                .map(|snapshot| snapshot.bytes)
                .sum::<usize>()
                + bytes
                > MAX_BYTES
        {
            snapshots.clear();
        }
        snapshots.insert(
            table.to_owned(),
            Snapshot {
                source,
                rows,
                bytes,
            },
        );
    }
}

impl CatalogCache {
    pub fn get(&self, table: &str, id: &DocumentId, history: &History) -> Option<Row> {
        let state = self.0.lock().expect("catalog cache lock");
        state
            .entries
            .get(&(table.to_owned(), id.clone()))
            // Match snapshot bytes, not only change IDs: callers can hold different transactions.
            .filter(|entry| entry.history == *history)
            .map(|entry| entry.row.clone())
    }

    pub fn insert(&self, table: &str, id: DocumentId, history: History, row: Row) {
        let Ok(encoded) = postcard::to_allocvec(&row) else {
            return;
        };
        let bytes = history
            .iter()
            .map(|(key, bytes)| key.id().len() + bytes.len() + size_of::<DocumentChangeKey>())
            .sum::<usize>()
            + encoded.len()
            + row.values.len() * size_of::<value::Value>()
            + size_of::<Entry>();
        if bytes > MAX_BYTES {
            return;
        }
        let mut state = self.0.lock().expect("catalog cache lock");
        let key = (table.to_owned(), id);
        if let Some(previous) = state.entries.remove(&key) {
            state.bytes -= previous.bytes;
        }
        if state.entries.len() >= MAX_ENTRIES || state.bytes + bytes > MAX_BYTES {
            state.entries.clear();
            state.bytes = 0;
        }
        state.bytes += bytes;
        state.entries.insert(
            key,
            Entry {
                history,
                row,
                bytes,
            },
        );
    }
}
