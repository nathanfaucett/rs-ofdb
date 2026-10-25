// Hold only the adapter's completion observation, not native IndexedDB progress.
// This makes timeout independent of machine speed and preserves real commits.
export function holdNextCompletion(mode) {
    const original = IDBDatabase.prototype.transaction;
    let release;
    const released = new Promise(resolve => { release = resolve; });
    let complete;
    const completed = new Promise(resolve => { complete = resolve; });
    IDBDatabase.prototype.transaction = function (...args) {
        const tx = original.apply(this, args);
        if (tx.mode !== mode) return tx;
        IDBDatabase.prototype.transaction = original;
        tx.addEventListener('complete', () => complete());
        Object.defineProperty(tx, 'oncomplete', {
            set(handler) {
                tx.addEventListener('complete', event => {
                    released.then(() => handler.call(tx, event));
                });
            }
        });
        return tx;
    };
    return { completed, release() {
        IDBDatabase.prototype.transaction = original;
        release();
    }};
}
