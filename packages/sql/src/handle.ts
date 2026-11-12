/** Do not free a Rust handle while a generated async call still borrows it. */
export class Handle<T extends { free(): void }> {
  private value: T | undefined;
  private closed = false;
  private pending = 0;

  constructor(value: T) {
    this.value = value;
  }

  get(): T {
    if (this.closed || !this.value) {
      const error = new Error("SQL handle is closed");
      (error as any).kind = "query";
      throw error;
    }
    return this.value;
  }

  async run<R>(
    operation: (value: T) => Promise<R>,
    exclusive = false,
  ): Promise<R> {
    const value = this.get();
    if (exclusive && this.pending !== 0) {
      const error = new Error("SQL transaction is busy");
      (error as any).kind = "query";
      throw error;
    }
    this.pending++;
    try {
      return await operation(value);
    } finally {
      this.pending--;
      if (this.closed && this.pending === 0) this.release();
    }
  }

  take(): T {
    const value = this.get();
    if (this.pending !== 0) {
      const error = new Error("SQL transaction is busy");
      (error as any).kind = "query";
      throw error;
    }
    this.closed = true;
    this.value = undefined;
    return value;
  }

  close(): void {
    this.closed = true;
    if (this.pending === 0) this.release();
  }

  private release(): void {
    const value = this.value;
    this.value = undefined;
    value?.free();
  }
}
