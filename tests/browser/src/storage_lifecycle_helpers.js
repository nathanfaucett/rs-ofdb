/**
 * @module storage_lifecycle_helpers
 * IndexedDB schema fixtures, peer contexts, and lifecycle fault injection for browser tests.
 */

/**
 * Create an incompatible schema or metadata fixture containing a preservation sentinel.
 * @param {string} name - Fresh logical database name.
 * @param {string} kind - Storage kind used in the physical name and metadata.
 * @param {'version'|'missing'|'keypath'|'autoincrement'|'index'|'extra'|'format'|'missing-kind'|'kind'|'revision'} mode - Incompatibility to create.
 * @returns {Promise<void>} Resolves after the fixture connection is closed.
 */
export async function malformed(name, kind, mode) {
    const db = await new Promise((resolve, reject) => {
        const request = indexedDB.open(`ofdb:${kind.length}:${kind}:${name}`, mode === 'version' ? 2 : 1);
        request.onupgradeneeded = () => {
            for (const store of ['meta', 'tables', 'records']) {
                if (mode === 'missing' && store === 'records') continue;
                const options = mode === 'keypath' && store === 'records' ? {keyPath: 'secret'} :
                    mode === 'autoincrement' && store === 'records' ? {autoIncrement: true} : undefined;
                const objectStore = request.result.createObjectStore(store, options);
                if (mode === 'index' && store === 'records') objectStore.createIndex('foreign-index', 'secret');
            }
            if (mode === 'extra') request.result.createObjectStore('foreign-store');
            const meta = request.transaction.objectStore('meta');
            meta.put(mode === 'format' ? 'future' : 'ofdb-byte-storage-1', 'format');
            if (mode !== 'missing-kind') meta.put(mode === 'kind' ? 'wrong' : kind, 'kind');
            meta.put(mode === 'revision' ? '18446744073709551616' : '0', 'revision');
            meta.put('preserve-me', 'sentinel');
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
    db.close();
}

/**
 * Read the sentinel to verify a rejected open preserved the fixture's metadata.
 * @param {string} name - Logical fixture database name.
 * @param {string} kind - Storage kind used in the physical database name.
 * @returns {Promise<string|undefined>} Stored sentinel, or undefined if absent.
 */
export async function sentinel(name, kind) {
    const request = indexedDB.open(`ofdb:${kind.length}:${kind}:${name}`);
    const db = await new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
    const value = await new Promise((resolve, reject) => {
        const tx = db.transaction('meta');
        const result = tx.objectStore('meta').get('sentinel');
        tx.oncomplete = () => resolve(result.result);
        tx.onabort = () => reject(tx.error);
    });
    db.close();
    return value;
}

/**
 * Install the peer worker's lifecycle command handler; serialized into worker source.
 * Commands contain physical and action ('upgrade' or 'block'), or action 'release'.
 * Replies are 'upgraded', 'blocked', 'deleted', or an object with an error string.
 * Blocking peers hold an uncooperative connection until release closes it.
 * This separate context also works when the test runner is itself a worker.
 * @returns {void}
 */
function remote() {
    let db;
    /**
     * Upgrade a database, block its deletion, or release the blocking connection.
     * @param {MessageEvent<{physical: string, action: 'upgrade'|'block'}|{action: 'release'}>} event - Peer command.
     * @returns {void}
     */
    self.onmessage = event => {
        const {physical, action} = event.data;
        if (action === 'release') {
            db.close();
            return;
        }
        const request = indexedDB.open(physical, action === 'upgrade' ? 2 : 1);
        request.onsuccess = () => {
            db = request.result;
            if (action === 'upgrade') {
                db.close();
                self.postMessage('upgraded');
            } else {
                db.onversionchange = () => {}; // Deliberately uncooperative peer.
                const deletion = indexedDB.deleteDatabase(physical);
                deletion.onblocked = () => self.postMessage('blocked');
                deletion.onsuccess = () => self.postMessage('deleted');
                deletion.onerror = () => self.postMessage({error: String(deletion.error)});
            }
        };
        request.onerror = () => self.postMessage({error: String(request.error)});
    };
}

/**
 * Start a peer context that upgrades storage or holds a connection blocking deletion.
 * @param {string} name - Logical database name.
 * @param {string} kind - Storage kind used in the physical database name.
 * @param {'upgrade'|'block'} action - Peer operation to perform.
 * @returns {Promise<Worker|undefined>} Live worker once deletion is blocked, or
 * undefined after upgrade and termination. Rejects on failure or a fifteen-second timeout.
 */
export function context(name, kind, action) {
    const url = URL.createObjectURL(new Blob([`(${remote.toString()})()`], {type: 'text/javascript'}));
    const worker = new Worker(url);
    URL.revokeObjectURL(url);
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { worker.terminate(); reject(new Error('context timed out')); }, 15000);
        worker.onerror = event => { clearTimeout(timer); worker.terminate(); reject(new Error(event.message)); };
        worker.onmessage = event => {
            clearTimeout(timer);
            if (event.data.error) { worker.terminate(); reject(new Error(event.data.error)); }
            else if (action === 'upgrade') { worker.terminate(); resolve(); }
            else resolve(worker);
        };
        worker.postMessage({physical: `ofdb:${kind.length}:${kind}:${name}`, action});
    });
}

/**
 * Close the blocking peer's connection and terminate it after its deletion reply.
 * @param {Worker} worker - Live blocking worker returned by context.
 * @returns {Promise<void>} Resolves after deletion; rejects on a peer-reported
 * error or a fifteen-second timeout.
 */
export function release(worker) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { worker.terminate(); reject(new Error('release timed out')); }, 15000);
        worker.onmessage = event => {
            clearTimeout(timer); worker.terminate();
            if (event.data.error) reject(new Error(event.data.error)); else resolve();
        };
        worker.postMessage({action: 'release'});
    });
}

/**
 * Check database absence, aborting initial creation so the check leaves no database.
 * @param {string} name - Logical database name.
 * @param {string} kind - Storage kind used in the physical database name.
 * @returns {Promise<boolean>} True for a missing database, false for an existing one;
 * rejects if opening fails for a reason other than the aborted initial creation.
 */
export function absent(name, kind) {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(`ofdb:${kind.length}:${kind}:${name}`);
        let fresh = false;
        request.onupgradeneeded = event => { fresh = event.oldVersion === 0; request.transaction.abort(); };
        request.onerror = () => fresh ? resolve(true) : reject(request.error);
        request.onsuccess = () => { request.result.close(); resolve(false); };
    });
}

/**
 * Make the next native open throw SecurityError containing a test secret.
 * Restores open before throwing so tests can check error redaction without a lasting hook.
 * @returns {void}
 */
export function denyOpen() {
    const original = IDBFactory.prototype.open;
    IDBFactory.prototype.open = function() {
        IDBFactory.prototype.open = original;
        throw new DOMException('private-key-and-value-must-not-leak', 'SecurityError');
    };
}

/**
 * Set an existing test-kind database's revision metadata to the maximum u64 value.
 * @param {string} name - Logical database name under the test storage kind.
 * @returns {Promise<void>} Resolves after the metadata commit and connection close.
 */
export async function exhaust(name) {
    const request = indexedDB.open(`ofdb:4:test:${name}`);
    const db = await new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
    await new Promise((resolve, reject) => {
        const tx = db.transaction('meta', 'readwrite');
        tx.objectStore('meta').put('18446744073709551615', 'revision');
        tx.oncomplete = resolve;
        tx.onabort = () => reject(tx.error);
    });
    db.close();
}
