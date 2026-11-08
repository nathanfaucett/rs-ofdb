import {
  JsKvSyncTransport,
  type KvChangeSubscription,
  type Client as KvClient,
  Database as KvDatabase,
  KvSyncConfig,
  KvSyncRole,
} from "@ofsdk/kv";
import { LoopbackTransport } from "./transport.js";

export interface KvPeer {
  id: string;
  name: string;
  online: boolean;
  db: KvDatabase;
  client: KvClient;
  subscription: KvChangeSubscription | null;
  syncing: boolean;
  lastSync: string;
  error: string;
}

interface SyncSession {
  transportA: LoopbackTransport;
  transportB: LoopbackTransport;
  cancelled: boolean;
}

export class KvPeerNetwork {
  private peers = new Map<string, KvPeer>();
  private sessions = new Map<string, SyncSession>();
  private prefix: string;
  private counter = 0;
  private onChange: () => void = () => {};
  private syncLoops = new Map<string, { abort: () => void }>();

  constructor(prefix: string) {
    this.prefix = prefix;
  }

  setListener(fn: () => void): void {
    this.onChange = fn;
  }

  getPeers(): KvPeer[] {
    return [...this.peers.values()];
  }

  getPeer(id: string): KvPeer | undefined {
    return this.peers.get(id);
  }

  async addPeer(): Promise<KvPeer> {
    this.counter++;
    const id = `${this.prefix}-peer-${this.counter}`;
    const name = `Peer ${String.fromCharCode(64 + this.counter)}`;
    const db = await KvDatabase.open(`${this.prefix}-${id}`);
    const client = db.client();
    const subscription = await db.sync_changes();
    const peer: KvPeer = {
      id,
      name,
      online: true,
      db,
      client,
      subscription,
      syncing: false,
      lastSync: "",
      error: "",
    };
    this.peers.set(id, peer);
    this.onChange();
    await this.syncWithAll(peer);
    this.startSyncLoop(peer);
    return peer;
  }

  async removePeer(id: string): Promise<void> {
    const peer = this.peers.get(id);
    if (!peer) return;
    this.stopSyncLoop(id);
    for (const [key, session] of this.sessions) {
      if (key.includes(id)) {
        session.cancelled = true;
        session.transportA.close();
        session.transportB.close();
        this.sessions.delete(key);
      }
    }
    this.peers.delete(id);
    this.onChange();
  }

  async setOnline(id: string, online: boolean): Promise<void> {
    const peer = this.peers.get(id);
    if (!peer) return;
    peer.online = online;
    this.onChange();
    if (online) {
      await this.syncWithAll(peer);
      this.startSyncLoop(peer);
      // Also trigger sync on other online peers to pull changes
      for (const other of this.peers.values()) {
        if (other.id !== id && other.online) {
          void this.syncPair(other, peer);
        }
      }
    } else {
      this.stopSyncLoop(id);
      for (const [key, session] of this.sessions) {
        if (key.includes(id)) {
          session.cancelled = true;
          session.transportA.close();
          session.transportB.close();
          this.sessions.delete(key);
        }
      }
    }
  }

  async write(id: string, key: string, value: string): Promise<void> {
    const peer = this.peers.get(id);
    if (!peer) return;
    await peer.client.set(key, value);
    // Sync is handled automatically by the subscription loop
  }

  private startSyncLoop(peer: KvPeer): void {
    if (!peer.subscription || this.syncLoops.has(peer.id)) return;

    let aborted = false;
    const loop = async () => {
      while (!aborted && peer.online && peer.subscription) {
        try {
          const work = peer.subscription.select();
          if (!work) {
            await new Promise((r) => setTimeout(r, 2000));
            continue;
          }

          const others = [...this.peers.values()].filter(
            (p) => p.id !== peer.id && p.online
          );
          for (const target of others) {
            if (aborted || !peer.online || !target.online) break;
            await this.syncPair(peer, target);
          }

          peer.subscription.acknowledge(work);
        } catch (error) {
          if (!aborted) {
            const msg = error instanceof Error ? error.message : String(error);
            peer.error = msg;
            this.onChange();
            await new Promise((r) => setTimeout(r, 1000));
          }
        }
      }
    };

    const abort = () => {
      aborted = true;
    };

    this.syncLoops.set(peer.id, { abort });
    void loop();
  }

  private stopSyncLoop(id: string): void {
    const loop = this.syncLoops.get(id);
    if (loop) {
      loop.abort();
      this.syncLoops.delete(id);
    }
  }

  private async syncWithAll(source: KvPeer): Promise<void> {
    const others = [...this.peers.values()].filter(
      (p) => p.id !== source.id && p.online
    );
    for (const target of others) {
      await this.syncPair(source, target);
    }
  }

  private sessionKey(a: string, b: string): string {
    return [a, b].sort().join("|");
  }

  private async syncPair(a: KvPeer, b: KvPeer): Promise<void> {
    if (!a.online || !b.online) return;
    const key = this.sessionKey(a.id, b.id);
    const existing = this.sessions.get(key);
    if (existing && !existing.cancelled) return;

    const [tA, tB] = LoopbackTransport.pair();
    const session: SyncSession = {
      transportA: tA,
      transportB: tB,
      cancelled: false,
    };
    this.sessions.set(key, session);

    a.syncing = true;
    b.syncing = true;
    a.error = "";
    b.error = "";
    this.onChange();

    const makeConfig = () => {
      const c = new KvSyncConfig();
      c.max_snapshots_per_batch = 10;
      return c;
    };
    const checkFn = () => Promise.resolve(undefined);

    const roleA = a.id < b.id ? KvSyncRole.Initiator : KvSyncRole.Responder;
    const roleB = a.id < b.id ? KvSyncRole.Responder : KvSyncRole.Initiator;

    try {
      await Promise.all([
        a.db.synchronize_checked(
          new JsKvSyncTransport(tA),
          roleA,
          makeConfig(),
          checkFn
        ),
        b.db.synchronize_checked(
          new JsKvSyncTransport(tB),
          roleB,
          makeConfig(),
          checkFn
        ),
      ]);
      const now = new Date().toLocaleTimeString();
      a.lastSync = `Synced at ${now}`;
      b.lastSync = `Synced at ${now}`;
      this.onChange();
    } catch (error) {
      if (!session.cancelled) {
        const msg = error instanceof Error ? error.message : String(error);
        a.error = msg;
        b.error = msg;
      }
    } finally {
      if (this.sessions.get(key) === session) {
        this.sessions.delete(key);
      }
      tA.close();
      tB.close();
      a.syncing = false;
      b.syncing = false;
      this.onChange();
    }
  }
}
