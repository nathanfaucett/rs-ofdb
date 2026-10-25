const stores = ['meta', 'tables', 'records'];
const format = 'ofdb-byte-storage-1';

export function open(name) {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(name, 1);
        let refused = false;
        request.onblocked = () => {
            refused = true;
            reject(new Error('IndexedDB upgrade blocked; close other connections'));
        };
        request.onerror = () => reject(request.error);
        request.onupgradeneeded = event => {
            if (event.oldVersion !== 0) {
                request.transaction.abort();
                return;
            }
            for (const store of stores) request.result.createObjectStore(store);
            const meta = request.transaction.objectStore('meta');
            meta.put(format, 'format');
            meta.put('0', 'revision');
        };
        request.onsuccess = () => {
            const db = request.result;
            db.onversionchange = () => db.close();
            if (refused) db.close();
            else resolve(db);
        };
    });
}

export function close(db) { db.close(); }

function observe(tx, resolve, reject, result) {
    tx.oncomplete = () => resolve(result());
    tx.onabort = () => reject(tx.error || new Error('IndexedDB transaction aborted'));
    // Errors bubble to the transaction and abort by default. Only abort/complete
    // is terminal: request success is never treated as persistence completion.
}

function metadata(meta) {
    return [meta.get('format'), meta.get('revision')];
}

function valid(marker, revision) {
    if (marker !== format || typeof revision !== 'string' || !/^(0|[1-9][0-9]*)$/.test(revision))
        throw new Error('Unsupported IndexedDB metadata format');
    if (BigInt(revision) > 18446744073709551615n)
        throw new Error('IndexedDB revision out of range');
}

export function snapshot(db) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(stores, 'readonly');
        const [marker, revision] = metadata(tx.objectStore('meta'));
        const tables = tx.objectStore('tables').getAllKeys();
        const keys = tx.objectStore('records').getAllKeys();
        const values = tx.objectStore('records').getAll();
        observe(tx, resolve, reject, () => {
            try {
                valid(marker.result, revision.result);
                return [revision.result, tables.result, keys.result, values.result];
            } catch (error) { reject(error); }
        });
    });
}

export function commit(db, expected, tables, records) {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(stores, 'readwrite');
        let failure;
        observe(tx, resolve, error => reject(failure || error), () => undefined);
        const meta = tx.objectStore('meta');
        const [marker, revision] = metadata(meta);
        // Both requests are queued together. Dependents are queued synchronously
        // in the final request's active success event, never after an await.
        revision.onsuccess = () => {
            try {
                valid(marker.result, revision.result);
                if (revision.result !== expected) {
                    failure = 'ofdb-revision-conflict';
                    tx.abort();
                    return;
                }
                if (BigInt(expected) === 18446744073709551615n)
                    throw new Error('IndexedDB revision exhausted');
                const names = tx.objectStore('tables');
                for (const [name, present] of tables) {
                    if (present) names.put(true, name);
                    else names.delete(name);
                }
                const bytes = tx.objectStore('records');
                for (const [table, key, value] of records) {
                    const id = [table, key];
                    if (value === null) bytes.delete(id);
                    else bytes.put(value, id);
                }
                meta.put((BigInt(expected) + 1n).toString(), 'revision');
            } catch (error) {
                failure = error;
                tx.abort();
            }
        };
    });
}
