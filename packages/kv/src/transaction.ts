import type { Entry } from "./client.js";
import type { DbValue } from "./value.js";
import type { WasmKvTransaction } from "./wasm.js";

/** KV transaction — holds operations until commit or rollback. */
export class Transaction {
  private wasm: WasmKvTransaction;

  constructor(wasm: WasmKvTransaction) {
    this.wasm = wasm;
  }

  async get<T extends DbValue = DbValue>(key: string): Promise<T | undefined> {
    try {
      const raw = await this.wasm.get(key);
      return (raw === undefined ? undefined : JSON.parse(raw)) as T | undefined;
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  async set(key: string, value: DbValue, expiresAt?: number): Promise<void> {
    try {
      await this.wasm.set(key, JSON.stringify(value), expiresAt);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await this.wasm.delete(key);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  async scan(start: string, end: string): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scan(start, end);
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  async scanPrefix(prefix: string): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scanPrefix(prefix);
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  async scanAll(): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scanAll();
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Commit all pending operations. */
  async commit(): Promise<void> {
    try {
      await this.wasm.commit();
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Discard all pending operations. */
  async rollback(): Promise<void> {
    try {
      await this.wasm.rollback();
    } catch (error) {
      throw wrapError(error, Error);
    }
  }
}
