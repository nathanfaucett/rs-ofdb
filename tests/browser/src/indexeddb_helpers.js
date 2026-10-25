/**
 * @module indexeddb_helpers
 * Native IndexedDB comparisons, fault injection, and fresh-context KV persistence probes.
 */

/**
 * Compare valid IndexedDB keys using the browser's native ordering.
 * @param {IDBValidKey} a - First key.
 * @param {IDBValidKey} b - Second key.
 * @returns {number} Negative, zero, or positive when a sorts before, with, or after b.
 * @throws {DOMException} If either argument is not a valid IndexedDB key.
 */
export function compare(a, b) { return indexedDB.cmp(a, b); }

/**
 * Inject a records-store commit failure through the native put prototype.
 * Restores put before aborting after the first successful request or throwing on
 * the second request, depending on mode. Only the test harness uses this hook.
 * @param {'abort'|'throw'} mode - Abort the transaction or throw QuotaExceededError.
 * @returns {void}
 */
export function failNextCommit(mode) {
    const original = IDBObjectStore.prototype.put;
    let count = 0;
    IDBObjectStore.prototype.put = function (...args) {
        if (this.name !== 'records') return original.apply(this, args);
        count++;
        if (mode === 'throw' && count === 2) {
            IDBObjectStore.prototype.put = original;
            throw new DOMException('test quota failure', 'QuotaExceededError');
        }
        const request = original.apply(this, args);
        if (mode === 'abort') {
            IDBObjectStore.prototype.put = original;
            const tx = this.transaction;
            request.addEventListener('success', () => tx.abort());
        }
        return request;
    };
}

/**
 * Run the KV restart probe in a fresh module worker and terminate it afterward.
 * @param {string} name - Logical KV database name.
 * @param {boolean} write - Whether to write fixtures instead of reading them.
 * @returns {Promise<string>} Probe result; rejects on probe failure, worker error,
 * or a thirty-second timeout.
 */
export function restartWorker(name, write) {
    // wasm-bindgen-test serves this module and its Wasm at the origin root.
    const root = new URL(import.meta.url).origin;
    const source = `
        import init, { restart_probe } from '${root}/wasm-bindgen-test';
        /**
         * Initialize Wasm and reply with the KV probe result or error.
         * @param {MessageEvent<[string, boolean]>} event - Database name and write flag.
         * @returns {Promise<void>}
         */
        self.onmessage = async event => {
            try {
                await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
                self.postMessage({ok: await restart_probe(...event.data)});
            } catch (error) { self.postMessage({error: String(error)}); }
        };
    `;
    const url = URL.createObjectURL(new Blob([source], {type: 'text/javascript'}));
    return new Promise((resolve, reject) => {
        const worker = new Worker(url, {type: 'module'});
        const timer = setTimeout(() => finish(new Error('restart worker timed out')), 30000);
        /**
         * Clean up the worker and settle its probe promise.
         * @param {Error|null} error - Failure, or null for success.
         * @param {string} [value] - Successful probe result.
         * @returns {void}
         */
        function finish(error, value) {
            clearTimeout(timer);
            worker.terminate();
            URL.revokeObjectURL(url);
            if (error) reject(error); else resolve(value);
        }
        worker.onerror = event => finish(new Error(event.message));
        worker.onmessage = event => {
            if ('error' in event.data) finish(new Error(event.data.error));
            else finish(null, event.data.ok);
        };
        worker.postMessage([name, write]);
    });
}

/**
 * Create a KV database with unsupported format metadata for open-rejection tests.
 * @param {string} name - Fresh logical KV database name.
 * @param {'version'|'format'} mode - Also use schema version two for version tests.
 * @returns {Promise<void>} Resolves after opening and closing the fixture database.
 */
export async function malformed(name, mode) {
    const physical = `ofdb:2:kv:${name}`;
    const db = await new Promise((resolve, reject) => {
        const req = indexedDB.open(physical, mode === 'version' ? 2 : 1);
        req.onupgradeneeded = () => {
            for (const table of ['meta', 'tables', 'records']) req.result.createObjectStore(table);
            const meta = req.transaction.objectStore('meta');
            meta.put('unsupported', 'format');
            meta.put('0', 'revision');
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
    db.close();
}

/**
 * Make the next native IndexedDB open throw SecurityError, restoring open first.
 * @returns {void}
 */
export function denyOpenOnce() {
    const original = IDBFactory.prototype.open;
    IDBFactory.prototype.open = function(...args) {
        IDBFactory.prototype.open = original;
        throw new DOMException('test access denied', 'SecurityError');
    };
}

/**
 * Run the KV restart probe in a fresh same-origin iframe and remove it afterward.
 * @param {string} name - Logical KV database name.
 * @param {boolean} write - Whether to write fixtures instead of reading them.
 * @returns {Promise<string>} Probe result; rejects on a reported failure or
 * a thirty-second timeout.
 */
export function restartPage(name, write) {
    const root = new URL(import.meta.url).origin;
    return new Promise((resolve, reject) => {
        const frame = document.createElement('iframe');
        const timer = setTimeout(() => finish(new Error('restart page timed out')), 30000);
        /**
         * Remove the iframe and listener, then settle the probe promise.
         * @param {Error|null} error - Failure, or null for success.
         * @param {string} [value] - Successful probe result.
         * @returns {void}
         */
        function finish(error, value) {
            clearTimeout(timer);
            window.removeEventListener('message', onmessage);
            frame.remove();
            if (error) reject(error); else resolve(value);
        }
        /**
         * Settle the probe only for replies from this iframe.
         * @param {MessageEvent<{ok: string}|{error: string}>} event - Probe reply.
         * @returns {void}
         */
        function onmessage(event) {
            if (event.source !== frame.contentWindow) return;
            if ('error' in event.data) finish(new Error(event.data.error));
            else finish(null, event.data.ok);
        }
        window.addEventListener('message', onmessage);
        frame.srcdoc = `<script type="module">
            import init, { restart_probe } from '${root}/wasm-bindgen-test';
            try {
                await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
                parent.postMessage({ok: await restart_probe(${JSON.stringify(name)}, ${write})}, '${root}');
            } catch (error) { parent.postMessage({error: String(error)}, '${root}'); }
        </script>`;
        document.body.append(frame);
    });
}

/**
 * Stage two independent worker transactions before allowing either to commit.
 * @param {string} name - Logical KV database shared by both workers.
 * @returns {Promise<Array<'committed'|'conflict'>>} Commit statuses in arrival order;
 * rejects on a reported failure, worker error, or a thirty-second timeout.
 */
export function raceWorkers(name) {
    const root = new URL(import.meta.url).origin;
    const source = `
        import init, { race_begin } from '${root}/wasm-bindgen-test';
        let tx;
        /**
         * Stage a transaction and signal readiness, or commit the staged transaction.
         * Replies with ready, result, or error fields.
         * @param {MessageEvent<{stage: true, name: string, key: string}|{commit: true}>} event - Race command.
         * @returns {Promise<void>}
         */
        self.onmessage = async event => {
            try {
                if (event.data.stage) {
                    await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
                    tx = await race_begin(event.data.name, event.data.key);
                    self.postMessage({ready: true});
                } else self.postMessage({result: await tx.commit()});
            } catch (error) { self.postMessage({error: String(error)}); }
        };
    `;
    return new Promise((resolve, reject) => {
        const url = URL.createObjectURL(new Blob([source], {type: 'text/javascript'}));
        const workers = [new Worker(url, {type: 'module'}), new Worker(url, {type: 'module'})];
        let ready = 0;
        const results = [];
        const timer = setTimeout(() => finish(new Error('race workers timed out')), 30000);
        /**
         * Terminate both workers, release their URL, and settle the race promise.
         * @param {Error} [error] - Failure; omitted after both results arrive.
         * @returns {void}
         */
        function finish(error) {
            clearTimeout(timer); workers.forEach(w => w.terminate()); URL.revokeObjectURL(url);
            if (error) reject(error); else resolve(results);
        }
        workers.forEach((worker, i) => {
            worker.onerror = event => finish(new Error(event.message));
            worker.onmessage = event => {
                if (event.data.error) finish(new Error(event.data.error));
                else if (event.data.ready) {
                    if (++ready === 2) workers.forEach(w => w.postMessage({commit: true}));
                } else {
                    results.push(event.data.result);
                    if (results.length === 2) finish();
                }
            };
            worker.postMessage({stage: true, name, key: String(i)});
        });
    });
}

/**
 * Upgrade a KV database to version two to exercise owned-connection closure.
 * @param {string} name - Logical KV database name.
 * @returns {Promise<void>} Resolves after closing the upgraded connection;
 * rejects if opening fails or an existing connection blocks the upgrade.
 */
export function upgrade(name) {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(`ofdb:2:kv:${name}`, 2);
        req.onsuccess = () => { req.result.close(); resolve(); };
        req.onerror = () => reject(req.error);
        req.onblocked = () => reject(new Error('owned connection did not close on versionchange'));
    });
}
