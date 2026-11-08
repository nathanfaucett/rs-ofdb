import { Transaction } from "./transaction.js";
import type { DbValue } from "./value.js";
import type { WasmKvClient } from "./wasm.js";

/** One KV entry returned by scan operations. */
export interface Entry {
  key: string;
  value: DbValue;
}

/** KV query handle — the only API for KV reads and writes. */
export class Client {
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
      throw wrapError(error, Error);
    }
  }

  /** Commit a value, optionally with an expiry timestamp (Unix epoch millis). */
  async set(key: string, value: DbValue, expiresAt?: number): Promise<void> {
    try {
      await this.wasm.set(key, JSON.stringify(value), expiresAt);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Remove a committed value. */
  async delete(key: string): Promise<void> {
    try {
      await this.wasm.delete(key);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Scan entries with keys in [start, end). */
  async scan(start: string, end: string): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scan(start, end);
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Scan entries whose keys start with the given prefix. */
  async scanPrefix(prefix: string): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scanPrefix(prefix);
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Scan all entries. */
  async scanAll(): Promise<Entry[]> {
    try {
      const raw = await this.wasm.scanAll();
      return JSON.parse(raw) as Entry[];
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Start a transaction that holds operations until commit or rollback. */
  async transaction(): Promise<Transaction> {
    try {
      const wasm = await this.wasm.transaction();
      return new Transaction(wasm);
    } catch (error) {
      throw wrapError(error, Error);
    }
  }

  /** Return a clone of this client with a request deadline in milliseconds. */
  withDeadline(ms: number): Client {
    return new Client(this.wasm.withDeadline(ms));
  }

  /** Return an independent clone sharing the same storage. */
  clone(): Client {
    return new Client(this.wasm.cloneClient());
  }
}
