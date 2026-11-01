import { KvError, wrapError } from "../error.js";
import type { DbValue } from "../value.js";
import type { WasmKvTransaction } from "../wasm.js";
import type { KVEntry } from "./client.js";

/** KV transaction — holds operations until commit or rollback. */
export class KvTransaction {
  constructor(private wasm: WasmKvTransaction) {}

  async get<T extends DbValue = DbValue>(key: string): Promise<T | undefined> {
    try {
      const raw = await this.wasm.get(key);
      return (raw === undefined ? undefined : JSON.parse(raw)) as T | undefined;
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  async set(key: string, value: DbValue, expiresAt?: number): Promise<void> {
    try {
      await this.wasm.set(key, JSON.stringify(value), expiresAt);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await this.wasm.delete(key);
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  async scan(start: string, end: string): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scan(start, end);
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  async scanPrefix(prefix: string): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scanPrefix(prefix);
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  async scanAll(): Promise<KVEntry[]> {
    try {
      const raw = await this.wasm.scanAll();
      return JSON.parse(raw) as KVEntry[];
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Commit all pending operations. */
  async commit(): Promise<void> {
    try {
      await this.wasm.commit();
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }

  /** Discard all pending operations. */
  async rollback(): Promise<void> {
    try {
      await this.wasm.rollback();
    } catch (error) {
      throw wrapError(error, KvError);
    }
  }
}
