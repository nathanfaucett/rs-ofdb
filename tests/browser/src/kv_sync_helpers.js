/**
 * @module kv_sync_helpers
 * Test-only observation of incoming IndexedDB writes during sync cancellation.
 */

/**
 * Observe the next successfully issued put request to the records store.
 * Restores the put prototype on observation or timeout; does not await commit.
 * @returns {Promise<void>} Resolves when the request is issued, or rejects after
 * ten seconds if no matching write is observed.
 */
export function afterNextWrite() {
    return new Promise((resolve, reject) => {
        const original = IDBObjectStore.prototype.put;
        const timer = setTimeout(() => {
            IDBObjectStore.prototype.put = original;
            reject(new Error('incoming commit was not issued'));
        }, 10000);
        IDBObjectStore.prototype.put = function (...args) {
            const request = original.apply(this, args);
            if (this.name === 'records') {
                IDBObjectStore.prototype.put = original;
                clearTimeout(timer);
                resolve();
            }
            return request;
        };
    });
}
