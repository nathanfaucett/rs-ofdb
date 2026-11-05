import {
  Database as SqlDatabase,
  type Client as SqlClient,
  JsSqlSyncTransport,
  SqlSessionConfig,
  SqlSyncRole,
} from "@ofsdk/sql";
import { LoopbackTransport } from "./transport.js";

export interface SqlPeer {
  id: string;
  name: string;
  online: boolean;
  db: SqlDatabase;
  client: SqlClient;
  syncing: boolean;
  lastSync: string;
  error: string;
  recordCount: number;
}

interface SyncSession {
  transportA: LoopbackTransport;
  transportB: LoopbackTransport;
  cancelled: boolean;
}

export class SqlPeerNetwork {
  private peers = new Map<string, SqlPeer>();
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

  getPeers(): SqlPeer[] {
    return [...this.peers.values()];
  }

  getPeer(id: string): SqlPeer | undefined {
    return this.peers.get(id);
  }

  async addPeer(): Promise<SqlPeer> {
    this.counter++;
    const id = `${this.prefix}-peer-${this.counter}`;
    const name = `Peer ${String.fromCharCode(64 + this.counter)}`;
    const db = await SqlDatabase.open(`${this.prefix}-${id}`);
    const client = db.client();
    await client.execute(
      "CREATE TABLE IF NOT EXISTS records (id UUID PRIMARY KEY, text TEXT)",
    );
    const peer: SqlPeer = {
      id,
      name,
      online: true,
      db,
      client,
      syncing: false,
      lastSync: "",
      error: "",
      recordCount: 0,
    };
    this.peers.set(id, peer);
    this.onChange();
    await this.refreshCount(peer);
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
      await this.refreshCount(peer);
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

  async addRecord(id: string, text: string): Promise<void> {
    const peer = this.peers.get(id);
    if (!peer) return;
    await peer.client.execute("INSERT INTO records (text) VALUES ($1)", [
      { type: "text", value: text },
    ]);
    await this.refreshCount(peer);
    // Sync is handled automatically by the polling loop
  }

  private startSyncLoop(peer: SqlPeer): void {
    if (this.syncLoops.has(peer.id)) return;

    let aborted = false;
    let lastRecordCount = peer.recordCount;
    let lastPeriodicSync = 0;

    const loop = async () => {
      while (!aborted && peer.online) {
        try {
          // Check for local changes by comparing record count
          const currentCount = await this.getRecordCount(peer);
          if (currentCount !== lastRecordCount) {
            lastRecordCount = currentCount;
            peer.recordCount = currentCount;
            this.onChange();

            // Sync with all online peers
            const others = [...this.peers.values()].filter(
              (p) => p.id !== peer.id && p.online,
            );
            for (const target of others) {
              if (aborted || !peer.online || !target.online) break;
              await this.syncPair(peer, target);
            }
          } else {
            // Periodic full sync every 10s to catch remote changes
            const now = Date.now();
            if (now - lastPeriodicSync > 10000) {
              lastPeriodicSync = now;
              const others = [...this.peers.values()].filter(
                (p) => p.id !== peer.id && p.online,
              );
              for (const target of others) {
                if (aborted || !peer.online || !target.online) break;
                await this.syncPair(peer, target);
              }
            }
          }
        } catch (error) {
          if (!aborted) {
            const msg = error instanceof Error ? error.message : String(error);
            peer.error = msg;
            this.onChange();
          }
        }
        await new Promise((r) => setTimeout(r, 2000));
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

  private async getRecordCount(peer: SqlPeer): Promise<number> {
    try {
      const results = await peer.client.execute(
        "SELECT COUNT(*) as cnt FROM records",
      );
      const first = results[0]?.rows?.[0]?.[0];
      if (first && typeof first === "object" && "value" in first) {
        return Number((first as { value: unknown }).value);
      }
      return Number(first) || 0;
    } catch {
      return 0;
    }
  }

  async refreshCount(peer: SqlPeer): Promise<void> {
    try {
      peer.recordCount = (await this.listRecords(peer)).length;
    } catch {
      peer.recordCount = 0;
    }
    this.onChange();
  }

  async listRecords(peer: SqlPeer): Promise<string[]> {
    const results = await peer.client.execute(
      "SELECT text FROM records ORDER BY id",
    );
    return (results[0]?.rows ?? []).map((row) => {
      const first = row[0];
      if (first && typeof first === "object" && "value" in first) {
        return String((first as { value: unknown }).value);
      }
      return typeof first === "string" ? first : JSON.stringify(first);
    });
  }

  private async syncWithAll(source: SqlPeer): Promise<void> {
    const others = [...this.peers.values()].filter(
      (p) => p.id !== source.id && p.online,
    );
    for (const target of others) {
      await this.syncPair(source, target);
    }
  }

  private sessionKey(a: string, b: string): string {
    return [a, b].sort().join("|");
  }

  private async syncPair(a: SqlPeer, b: SqlPeer): Promise<void> {
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
      const c = new SqlSessionConfig();
      c.max_units_per_frame = 100;
      return c;
    };
    const checkFn = () => Promise.resolve(undefined);

    const roleA = a.id < b.id ? SqlSyncRole.Initiator : SqlSyncRole.Responder;
    const roleB = a.id < b.id ? SqlSyncRole.Responder : SqlSyncRole.Initiator;

    try {
      await Promise.all([
        a.db.synchronize_checked(
          new JsSqlSyncTransport(tA),
          makeConfig(),
          roleA,
          checkFn,
        ),
        b.db.synchronize_checked(
          new JsSqlSyncTransport(tB),
          makeConfig(),
          roleB,
          checkFn,
        ),
      ]);
      const now = new Date().toLocaleTimeString();
      a.lastSync = `Synced at ${now}`;
      b.lastSync = `Synced at ${now}`;
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
