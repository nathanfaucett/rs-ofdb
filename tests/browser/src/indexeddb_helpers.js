export function compare(a, b) { return indexedDB.cmp(a, b); }

// Fault injection lives only in the consumer harness. Abort after a real write
// request succeeds, or throw after one request has already been queued.
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

export function restartWorker(name, write) {
    // wasm-bindgen-test serves this module and its Wasm at the origin root.
    const root = new URL(import.meta.url).origin;
    const source = `
        import init, { restart_probe } from '${root}/wasm-bindgen-test';
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

export function denyOpenOnce() {
    const original = IDBFactory.prototype.open;
    IDBFactory.prototype.open = function(...args) {
        IDBFactory.prototype.open = original;
        throw new DOMException('test access denied', 'SecurityError');
    };
}

export function restartPage(name, write) {
    const root = new URL(import.meta.url).origin;
    return new Promise((resolve, reject) => {
        const frame = document.createElement('iframe');
        const timer = setTimeout(() => finish(new Error('restart page timed out')), 30000);
        function finish(error, value) {
            clearTimeout(timer);
            window.removeEventListener('message', onmessage);
            frame.remove();
            if (error) reject(error); else resolve(value);
        }
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

export function raceWorkers(name) {
    const root = new URL(import.meta.url).origin;
    const source = `
        import init, { race_begin } from '${root}/wasm-bindgen-test';
        let tx;
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

export function upgrade(name) {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(`ofdb:2:kv:${name}`, 2);
        req.onsuccess = () => { req.result.close(); resolve(); };
        req.onerror = () => reject(req.error);
        req.onblocked = () => reject(new Error('owned connection did not close on versionchange'));
    });
}
