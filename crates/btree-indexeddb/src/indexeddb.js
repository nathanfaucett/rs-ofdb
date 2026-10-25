/**
 * Browser IndexedDB boundary imported by Rust through `platform.rs`/wasm-bindgen.
 * Rust owns staged snapshots; native transactions exist only for individual reads
 * and commits. Physical version 1 uses out-of-line keys and no indexes: `meta`
 * holds format/kind/revision strings, `tables` maps names to true, and `records`
 * maps [table name, binary key] pairs to bytes. Revisions cross the boundary as
 * canonical decimal u64 strings to avoid JavaScript number precision loss.
 *
 * Promises settle on transaction completion/abort, not individual write success.
 * Dropping a Rust future or ignoring a Promise does not cancel issued work; this
 * module exposes no cancellation API. Storage failures become OfdbStorageError
 * errors, except the literal 'ofdb-revision-conflict' consumed by Rust as Conflict.
 * @module
 */

/**
 * @typedef {Object} ConnectionState
 * @property {string} kind Expected logical store kind, supplied by Rust.
 * @property {boolean} closed Whether this handle has been marked unavailable.
 * @property {string} [reason] Diagnostic for version changes or unexpected closure.
 */

/** @typedef {[name: string, present: boolean]} TableChange */
/** @typedef {[table: string, key: Uint8Array, value: Uint8Array|null]} RecordChange */
/** @typedef {[table: string, key: ArrayBuffer]} RecordKey */
/**
 * Parallel record keys and values are in IndexedDB key order. Binary keys are
 * returned as ArrayBuffers; Rust accepts Uint8Array or ArrayBuffer record values.
 * This describes the expected byte-storage data, not validation of every record.
 * @typedef {[revision: string, tables: string[], keys: RecordKey[], values: (Uint8Array|ArrayBuffer)[]]} Snapshot
 */

const stores = ['meta', 'tables', 'records'];
const format = 'ofdb-byte-storage-1';
/** @type {WeakMap<IDBDatabase, ConnectionState>} */
const connections = new WeakMap();

/**
 * Creates an error that storageError preserves without wrapping again.
 * @param {string} message Actionable storage diagnostic.
 * @returns {Error} Error named OfdbStorageError.
 */
function failure(message) {
    const error = new Error(message);
    error.name = 'OfdbStorageError';
    return error;
}

/**
 * Normalizes browser failures while preserving our errors and conflict sentinel.
 * @param {*} error Thrown value or native request/transaction error.
 * @param {string} operation Operation name included in the diagnostic.
 * @returns {Error|'ofdb-revision-conflict'} Failure passed to the Rust boundary.
 */
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

/**
 * Opens or creates a physical version-1 database and validates its schema/metadata.
 * Blocked opens fail immediately; a five-second deadline also bounds queued opens
 * and validation. A late connection after failure is closed, never returned.
 * Version changes close the handle; unexpected closure marks it unavailable.
 * Neither event triggers implicit reopening or migration.
 * @param {string} name Physical database name, already namespaced by Rust.
 * @param {string} kind Logical store kind to initialize or match exactly.
 * @returns {Promise<IDBDatabase>} Registered handle after read transaction completion;
 * rejects with a normalized storage error on opening or validation failure.
 */
export function open(name, kind) {
    return new Promise((resolve, reject) => {
        let request;
        let db;
        let refused = false;
        /**
         * Rejects opening, clears its deadline, and closes any acquired connection.
         * @param {*} error Opening or validation failure.
         * @returns {void}
         */
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

/**
 * Marks a handle unavailable and requests native closure. Already-issued
 * transactions may still complete; closing does not abort or cancel them.
 * @param {IDBDatabase} db Connection, normally obtained from open.
 * @returns {void}
 */
export function close(db) {
    const state = connections.get(db);
    if (state) state.closed = true;
    db.close();
}

/**
 * Checks tracked lifecycle state without probing browser storage availability.
 * @param {IDBDatabase} db Connection obtained from open.
 * @returns {void}
 * @throws {Error} OfdbStorageError if unregistered or marked closed.
 */
export function ensureOpen(db) {
    const state = connections.get(db);
    if (!state || state.closed) throw failure(state?.reason || 'IndexedDB connection closed; explicitly reopen');
}

/**
 * Settles an operation only on a terminal native transaction event. The result
 * callback runs after completion, when request results are available but no new
 * requests can be queued. A result callback must handle its own exceptions.
 * @template T
 * @param {IDBTransaction} tx Transaction whose terminal handlers are installed.
 * @param {function(T): void} resolve Promise resolver.
 * @param {function(*): void} reject Promise rejecter, optionally mapping errors.
 * @param {function(): T} result Computes the successful boundary value.
 * @returns {void}
 */
function observe(tx, resolve, reject, result) {
    tx.oncomplete = () => resolve(result());
    tx.onabort = () => reject(tx.error || failure('IndexedDB transaction aborted'));
    // Errors bubble to the transaction and abort by default. Only abort/complete
    // is terminal: request success is never treated as persistence completion.
}

/**
 * Queues metadata reads in format/kind/revision order while the transaction is
 * active. Returns format/revision/kind order, making revision the final request
 * whose success handler can safely read all three results and queue writes.
 * @param {IDBObjectStore} meta Metadata store in an active transaction.
 * @returns {[IDBRequest<*>, IDBRequest<*>, IDBRequest<*>]} Format, revision, kind requests.
 */
function metadata(meta) {
    const marker = meta.get('format');
    const kind = meta.get('kind');
    return [marker, meta.get('revision'), kind];
}

/**
 * Validates the format marker, exact store kind, and canonical decimal u64 revision.
 * @param {*} marker Stored format marker.
 * @param {*} revision Stored revision; must be a decimal string without leading zeros.
 * @param {*} kind Stored logical store kind.
 * @param {string} expectedKind Kind registered for this connection.
 * @returns {void}
 * @throws {Error} OfdbStorageError for incompatible or out-of-range metadata.
 */
function valid(marker, revision, kind, expectedKind) {
    if (kind !== expectedKind) throw failure('Unsupported IndexedDB store kind; use a compatible database');
    if (marker !== format || typeof revision !== 'string' || !/^(0|[1-9][0-9]*)$/.test(revision))
        throw failure('Unsupported IndexedDB metadata format');
    if (BigInt(revision) > 18446744073709551615n)
        throw failure('IndexedDB revision out of range');
}

/**
 * Reads revision, table names, and all records in one consistent readonly
 * transaction, using memory proportional to the entire database contents.
 * @param {IDBDatabase} db Open, registered connection.
 * @returns {Promise<Snapshot>} Tuple for Rust reconstruction after completion;
 * rejects with a normalized storage error on lifecycle, read, or metadata failure.
 */
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

/**
 * Atomically applies Rust's staged delta if the stored revision matches expected,
 * then increments the revision. Metadata-dependent writes are queued synchronously
 * inside the last metadata request's active success event, never after an await.
 * Table removal only deletes the name: Rust must supply deletions for its records.
 * Once invoked, ignoring the Promise does not cancel the transaction.
 * @param {IDBDatabase} db Open, registered connection.
 * @param {string} expected Snapshot revision as a canonical decimal u64 string.
 * @param {TableChange[]} tables Table-name additions (true) and removals (false).
 * @param {RecordChange[]} records Byte upserts or deletions (null value).
 * @returns {Promise<void>} Resolves only after transaction completion. Rejects with
 * 'ofdb-revision-conflict' on mismatch, or a normalized storage error on failure;
 * an aborted transaction commits none of its changes.
 */
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
