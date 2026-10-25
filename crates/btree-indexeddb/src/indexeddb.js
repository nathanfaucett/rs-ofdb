const stores = ['meta', 'tables', 'records'];
const format = 'ofdb-byte-storage-1';
const connections = new WeakMap();

function failure(message) {
    const error = new Error(message);
    error.name = 'OfdbStorageError';
    return error;
}

function storageError(error, operation) {
    if (error === 'ofdb-revision-conflict' || error?.name === 'OfdbStorageError') return error;
    const hints = {
        SecurityError: 'allow storage access for this origin',
        NotAllowedError: 'allow storage access for this origin',
        QuotaExceededError: 'free origin storage or request persistent storage',
        AbortError: 'transaction aborted; no changes were committed',
        VersionError: 'unsupported physical version; use a compatible database',
        InvalidStateError: 'connection unavailable; explicitly open a new handle',
        UnknownError: 'browser storage unavailable; check browser storage settings',
    };
    const name = Object.hasOwn(hints, error?.name) ? error.name : 'StorageError';
    return failure(`IndexedDB ${operation}: ${name}; ${hints[name] || 'check browser storage availability'}`);
}

export function open(name, kind) {
    return new Promise((resolve, reject) => {
        let request;
        let db;
        let refused = false;
        const fail = error => {
            refused = true;
            clearTimeout(timer);
            if (db) close(db);
            reject(error);
        };
        // Also bounds requests queued behind another context's blocked deletion,
        // where this open request never receives its own blocked event.
        const timer = setTimeout(() => fail(failure('IndexedDB open timed out after 5000 ms; close other tabs/workers and retry explicitly')), 5000);
        try { request = indexedDB.open(name, 1); }
        catch (error) { fail(error); return; }
        request.onblocked = () => fail(failure('IndexedDB upgrade blocked; close other tabs/workers and retry explicitly'));
        request.onerror = () => fail(request.error);
        request.onupgradeneeded = event => {
            if (refused || event.oldVersion !== 0) {
                request.transaction.abort();
                return;
            }
            for (const store of stores) request.result.createObjectStore(store);
            const meta = request.transaction.objectStore('meta');
            meta.put(format, 'format');
            meta.put(kind, 'kind');
            meta.put('0', 'revision');
        };
        request.onsuccess = () => {
            db = request.result;
            if (refused) { db.close(); return; }
            const state = {kind, closed: false};
            connections.set(db, state);
            db.onversionchange = () => {
                state.reason = 'IndexedDB connection closed by version change; discard staged work and explicitly open a compatible database';
                close(db);
            };
            db.onclose = () => {
                state.reason = 'IndexedDB connection unexpectedly closed; check storage availability and explicitly reopen';
                state.closed = true;
            };
            try {
                if (db.version !== 1 || db.objectStoreNames.length !== stores.length ||
                    stores.some(name => !db.objectStoreNames.contains(name)))
                    throw failure('Unsupported IndexedDB physical schema; use a compatible database');
                const tx = db.transaction(stores);
                for (const name of stores) {
                    const store = tx.objectStore(name);
                    if (store.keyPath !== null || store.autoIncrement || store.indexNames.length)
                        throw failure('Unsupported IndexedDB object-store schema; use a compatible database');
                }
                const [marker, revision, storedKind] = metadata(tx.objectStore('meta'));
                tx.onabort = () => fail(tx.error);
                tx.oncomplete = () => {
                    if (refused) return;
                    try {
                        ensureOpen(db);
                        valid(marker.result, revision.result, storedKind.result, kind);
                        clearTimeout(timer);
                        resolve(db);
                    } catch (error) { fail(error); }
                };
            } catch (error) { fail(error); }
        };
    }).catch(error => { throw storageError(error, 'open'); });
}

export function close(db) {
    const state = connections.get(db);
    if (state) state.closed = true;
    db.close();
}

export function ensureOpen(db) {
    const state = connections.get(db);
    if (!state || state.closed) throw failure(state?.reason || 'IndexedDB connection closed; explicitly reopen');
}

function observe(tx, resolve, reject, result) {
    tx.oncomplete = () => resolve(result());
    tx.onabort = () => reject(tx.error || failure('IndexedDB transaction aborted'));
    // Errors bubble to the transaction and abort by default. Only abort/complete
    // is terminal: request success is never treated as persistence completion.
}

function metadata(meta) {
    const marker = meta.get('format');
    const kind = meta.get('kind');
    return [marker, meta.get('revision'), kind];
}

function valid(marker, revision, kind, expectedKind) {
    if (kind !== expectedKind) throw failure('Unsupported IndexedDB store kind; use a compatible database');
    if (marker !== format || typeof revision !== 'string' || !/^(0|[1-9][0-9]*)$/.test(revision))
        throw failure('Unsupported IndexedDB metadata format');
    if (BigInt(revision) > 18446744073709551615n)
        throw failure('IndexedDB revision out of range');
}

export function snapshot(db) {
    return new Promise((resolve, reject) => {
        ensureOpen(db);
        const tx = db.transaction(stores, 'readonly');
        const [marker, revision, kind] = metadata(tx.objectStore('meta'));
        const tables = tx.objectStore('tables').getAllKeys();
        const keys = tx.objectStore('records').getAllKeys();
        const values = tx.objectStore('records').getAll();
        observe(tx, resolve, reject, () => {
            try {
                valid(marker.result, revision.result, kind.result, connections.get(db).kind);
                return [revision.result, tables.result, keys.result, values.result];
            } catch (error) { reject(error); }
        });
    }).catch(error => { throw storageError(error, 'snapshot'); });
}

export function commit(db, expected, tables, records) {
    return new Promise((resolve, reject) => {
        ensureOpen(db);
        const tx = db.transaction(stores, 'readwrite');
        let commitFailure;
        observe(tx, resolve, error => reject(commitFailure || error), () => undefined);
        const meta = tx.objectStore('meta');
        const [marker, revision, kind] = metadata(meta);
        // Metadata requests are queued together. Dependents are queued synchronously
        // in the final request's active success event, never after an await.
        revision.onsuccess = () => {
            try {
                valid(marker.result, revision.result, kind.result, connections.get(db).kind);
                if (revision.result !== expected) {
                    commitFailure = 'ofdb-revision-conflict';
                    tx.abort();
                    return;
                }
                if (BigInt(expected) === 18446744073709551615n)
                    throw failure('IndexedDB revision exhausted');
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
                commitFailure = error;
                tx.abort();
            }
        };
    }).catch(error => { throw storageError(error, 'commit'); });
}
