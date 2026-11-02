import { KvError, wrapError } from "./error.js";
import { KvTransaction } from "./transaction.js";
import type { DbValue } from "./value.js";
import type { WasmKvClient } from "./wasm.js";

/** One KV entry returned by scan operations. */
export interface KVEntry {
  key: string;
  value: DbValue;
}

/** KV query handle — the only API for KV reads and writes. */
export class KvClient {
  private readonly wasm: WasmKvClient;

  constructor(wasm: WasmKvClient) {
    this.wasm = wasm;
  }

  /** Read a committed value, or undefined when the key is missing. */
  async get<T extends DbValue = DbValue>(key: string): Promise<T | undefined> {
    try {
      const raw = await this.wasm.get(key);
      return (raw === undefined ? undefined : JSON.parse(raw)) as T | undefined;
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Commit a value, optionally with an expiry timestamp (Unix epoch millis). */
  async set(key: string, value: DbValue, expiresAt?: number): Promise<void> {
    try {
      await this.wasm.set(key, JSON.stringify(value), expiresAt);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Remove a committed value. */
  async delete(key: string): Promise<void> {
    try {
      await this.wasm.delete(key);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Scan entries with keys in [start, end). */
  async scan(start: string, end: string): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scan(start, end);
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Scan entries whose keys start with the given prefix. */
  async scanPrefix(prefix: string): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scanPrefix(prefix);
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Scan all entries. */
  async scanAll(): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scanAll();
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Start a transaction that holds operations until commit or rollback. */
  async transaction(): Promise<KvTransaction> {
    try {
      const wasm = await this.wasm.transaction();
      return new KvTransaction(wasm);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Return a clone of this client with a request deadline in milliseconds. */
  withDeadline(ms: number): KvClient {
    return new KvClient(this.wasm.withDeadline(ms));
  }

  /** Return an independent clone sharing the same storage. */
  clone(): KvClient {
    return new KvClient(this.wasm.cloneClient());
  }
}
