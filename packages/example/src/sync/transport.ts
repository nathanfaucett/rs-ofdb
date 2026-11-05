/**
 * In-memory bidirectional transport pair for simulating peer-to-peer sync
 * between browser peers. Replace with WebRTC/WebSocket in production.
 */
export class LoopbackTransport {
    private queue: Uint8Array[] = [];
    private pendingResolve: ((value: Uint8Array) => void) | null = null;
    private pendingReject: ((error: Error) => void) | null = null;
    private peer: LoopbackTransport | null = null;
    closed = false;

    static pair(): [LoopbackTransport, LoopbackTransport] {
        const a = new LoopbackTransport();
        const b = new LoopbackTransport();
        a.peer = b;
        b.peer = a;
        return [a, b];
    }

    async send(frame: Uint8Array): Promise<void> {
        if (this.closed) throw new Error("transport closed");
        const target = this.peer;
        if (!target || target.closed) throw new Error("peer disconnected");
        target.deliver(frame);
    }

    async receive(): Promise<Uint8Array> {
        if (this.closed) throw new Error("transport closed");
        if (this.queue.length > 0) return this.queue.shift()!;
        return new Promise((resolve, reject) => {
            this.pendingResolve = resolve;
            this.pendingReject = reject;
        });
    }

    private deliver(frame: Uint8Array): void {
        if (this.closed) return;
        if (this.pendingResolve) {
            this.pendingResolve(frame);
            this.pendingResolve = null;
            this.pendingReject = null;
        } else {
            this.queue.push(frame);
        }
    }

    close(): void {
        this.closed = true;
        if (this.pendingReject) {
            this.pendingReject(new Error("transport closed"));
            this.pendingResolve = null;
            this.pendingReject = null;
        }
        if (this.peer) {
            this.peer.closed = true;
            if (this.peer.pendingReject) {
                this.peer.pendingReject(new Error("peer disconnected"));
                this.peer.pendingResolve = null;
                this.peer.pendingReject = null;
            }
            this.peer = null;
        }
    }
}
