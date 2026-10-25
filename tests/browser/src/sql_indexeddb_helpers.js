/**
 * @module sql_indexeddb_helpers
 * Fresh-context SQL persistence probes for the wasm-bindgen browser test harness.
 */

/**
 * Run the SQL restart probe in a fresh iframe or module worker, then clean it up.
 * @param {string} name - Logical SQL database name shared with the probe.
 * @param {boolean} write - Whether the probe writes fixtures instead of reading them.
 * @param {boolean} page - Use a same-origin iframe when true, otherwise a worker.
 * @returns {Promise<string>} Probe result; rejects on probe failure, worker error,
 * or a thirty-second timeout.
 */
export function restartSql(name, write, page) {
    const root = new URL(import.meta.url).origin;
    const call = `
        const {default: init, sql_restart_probe} = await import('${root}/wasm-bindgen-test');
        await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
    `;
    return new Promise((resolve, reject) => {
        let target, url;
        const timer = setTimeout(() => finish(new Error('SQL restart timed out')), 30000);
        /**
         * Remove the execution context and settle the probe promise.
         * @param {Error|null} error - Failure, or null for success.
         * @param {string} [value] - Successful probe result.
         * @returns {void}
         */
        function finish(error, value) {
            clearTimeout(timer);
            if (page) { window.removeEventListener('message', receive); target.remove(); }
            else { target.terminate(); URL.revokeObjectURL(url); }
            if (error) reject(error); else resolve(value);
        }
        /**
         * Handle a probe reply, ignoring messages from other iframe sources.
         * @param {MessageEvent<{ok: string}|{error: string}>} event - Probe reply.
         * @returns {void}
         */
        function receive(event) {
            if (page && event.source !== target.contentWindow) return;
            if ('error' in event.data) finish(new Error(event.data.error));
            else finish(null, event.data.ok);
        }
        if (page) {
            target = document.createElement('iframe');
            window.addEventListener('message', receive);
            target.srcdoc = `<script type="module">
                try { ${call}
                    parent.postMessage({ok: await sql_restart_probe(${JSON.stringify(name)}, ${write})}, '${root}');
                } catch (error) { parent.postMessage({error: String(error)}, '${root}'); }
            </script>`;
            document.body.append(target);
        } else {
            url = URL.createObjectURL(new Blob([`
                /**
                 * Initialize Wasm and reply with the SQL probe result or error.
                 * @param {MessageEvent<[string, boolean]>} event - Database name and write flag.
                 * @returns {Promise<void>}
                 */
                self.onmessage = async event => {
                    try {
                        const {default: init, sql_restart_probe} = await import('${root}/wasm-bindgen-test');
                        await init({module_or_path: '${root}/wasm-bindgen-test_bg.wasm'});
                        self.postMessage({ok: await sql_restart_probe(...event.data)});
                    } catch (error) { self.postMessage({error: String(error)}); }
                };
            `], {type: 'text/javascript'}));
            target = new Worker(url, {type: 'module'});
            target.onmessage = receive;
            target.onerror = event => finish(new Error(event.message));
            target.postMessage([name, write]);
        }
    });
}
