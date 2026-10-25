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
