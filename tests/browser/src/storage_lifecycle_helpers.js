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

// The other connection lives in a separate execution context, including when
// the test runner is itself a Dedicated Worker.
function remote() {
    let db;
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

export function absent(name, kind) {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(`ofdb:${kind.length}:${kind}:${name}`);
        let fresh = false;
        request.onupgradeneeded = event => { fresh = event.oldVersion === 0; request.transaction.abort(); };
        request.onerror = () => fresh ? resolve(true) : reject(request.error);
        request.onsuccess = () => { request.result.close(); resolve(false); };
    });
}

export function denyOpen() {
    const original = IDBFactory.prototype.open;
    IDBFactory.prototype.open = function() {
        IDBFactory.prototype.open = original;
        throw new DOMException('private-key-and-value-must-not-leak', 'SecurityError');
    };
}

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
