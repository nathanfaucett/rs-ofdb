<script lang="ts">
  import { KvStore, SqlStore } from "@of/db";

  let kv: KvStore | undefined = $state();
  let sql: SqlStore | undefined = $state();

  let key = $state("theme");
  let value = $state("");
  let status = $state("Loading Wasm and opening browser storage…");
  let statusError = $state(false);
  let result = $state("");
  let kvBusy = $state(true);

  let record = $state("");
  let sqlStatus = $state("Loading SQL storage…");
  let sqlStatusError = $state(false);
  let sqlResult: string[] = $state([]);
  let sqlBusy = $state(true);

  function message(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  function report(text: string, error = false): void {
    status = text;
    statusError = error;
  }

  async function operateKv(save: boolean): Promise<void> {
    if (kvBusy || !kv) return;
    if (save && !key.trim()) return;
    kvBusy = true;
    result = "";
    report(save ? "Saving…" : "Retrieving…");
    try {
      if (save) {
        await kv.set(key, value);
        report("Saved and committed.");
      } else {
        const stored = await kv.get(key);
        if (stored === undefined) {
          report("Key not found.");
        } else {
          result = typeof stored === "string" ? stored : JSON.stringify(stored);
          report("Retrieved committed value.");
        }
      }
    } catch (error) {
      report(
        `Error: ${message(error)}. Check browser storage permissions and available space; reload and retrieve before retrying a write.`,
        true,
      );
    } finally {
      kvBusy = false;
    }
  }

  async function listSql(): Promise<void> {
    if (!sql) return;
    const results = await sql.execute("SELECT text FROM records ORDER BY id");
    sqlResult = results.flatMap((r) =>
      r.rows.map((row) => {
        const first = row[0];
        return typeof first === "string" ? first : JSON.stringify(first);
      }),
    );
  }

  async function operateSql(add: boolean): Promise<void> {
    if (sqlBusy || !sql) return;
    if (add && !record.trim()) return;
    sqlBusy = true;
    sqlStatusError = false;
    sqlStatus = add ? "Adding…" : "Listing…";
    try {
      if (add) {
        await sql.execute("INSERT INTO records (text) VALUES ($1)", [record]);
        record = "";
      }
      await listSql();
      sqlStatus = add ? "Added and committed." : "Listed committed records.";
    } catch (error) {
      sqlStatusError = true;
      sqlStatus = `Error: ${message(error)}. Check browser storage permissions and available space; reload and list records before retrying a write.`;
    } finally {
      sqlBusy = false;
    }
  }

  async function startKv(): Promise<void> {
    try {
      kv = await KvStore.open("ofdb-browser-example-kv");
      kvBusy = false;
      await operateKv(false);
    } catch (error) {
      kvBusy = true;
      report(
        `Error: ${message(error)}. Check that the example is built and served over HTTP, allow browser storage, then reload. No memory fallback is used.`,
        true,
      );
    }
  }

  async function startSql(): Promise<void> {
    try {
      sql = await SqlStore.open("ofdb-browser-example-sql");
      await sql.execute(
        "CREATE TABLE IF NOT EXISTS records (id UUID PRIMARY KEY, text TEXT)",
      );
      sqlBusy = false;
      await operateSql(false);
    } catch (error) {
      sqlBusy = true;
      sqlStatusError = true;
      sqlStatus = `Error: ${message(error)}. Check the Wasm build and browser storage permissions, then reload. No memory fallback is used.`;
    }
  }

  $effect(() => {
    void startKv();
    void startSql();
  });
</script>

<main class="mx-auto max-w-2xl px-4 py-12 font-sans text-base leading-relaxed">
  <h1 class="mb-4 text-2xl font-bold">Persistent browser KV and SQL</h1>
  <p class="mb-8 text-gray-600">
    Save text in IndexedDB, reload, then retrieve it. The schema, queries and UI
    live in TypeScript; Rust is a generic Wasm bridge to the database engine.
  </p>

  <form
    autocomplete="off"
    onsubmit={(e) => {
      e.preventDefault();
      void operateKv(true);
    }}
  >
    <fieldset id="controls" disabled={kvBusy} class="mb-2 rounded-lg border border-gray-300 p-4">
      <legend class="px-1 font-semibold">KV store</legend>
      <label for="key" class="mb-1 block text-sm font-medium">KV Key</label>
      <input
        id="key"
        name="key"
        bind:value={key}
        required
        class="mb-3 w-full rounded border border-gray-300 px-2 py-1.5"
      />
      <label for="value" class="mb-1 block text-sm font-medium">Text value</label>
      <textarea
        id="value"
        name="value"
        bind:value
        rows="3"
        class="mb-3 w-full rounded border border-gray-300 px-2 py-1.5"
      ></textarea>
      <button
        id="save"
        type="submit"
        class="mr-2 rounded bg-blue-600 px-3 py-1.5 text-white hover:bg-blue-700 disabled:opacity-50"
      >Save</button>
      <button
        id="retrieve"
        type="button"
        onclick={() => void operateKv(false)}
        class="rounded border border-gray-300 px-3 py-1.5 hover:bg-gray-100 disabled:opacity-50"
      >Retrieve</button>
    </fieldset>
  </form>
  <p
    id="status"
    role="status"
    aria-live="polite"
    aria-atomic="true"
    data-error={String(statusError)}
    class={statusError ? "mb-4 font-bold" : "mb-4"}
  >{status}</p>
  <h2 class="mb-2 text-lg font-semibold">Committed value</h2>
  <pre
    id="result"
    class="mb-8 min-h-6 overflow-wrap-anywhere whitespace-pre-wrap rounded border border-gray-300 p-4"
  >{result}</pre>

  <h2 class="mb-2 text-lg font-semibold">SQL records</h2>
  <form
    id="sql-form"
    autocomplete="off"
    onsubmit={(e) => {
      e.preventDefault();
      void operateSql(true);
    }}
  >
    <fieldset id="sql-controls" disabled={sqlBusy} class="mb-2 rounded-lg border border-gray-300 p-4">
      <legend class="px-1 font-semibold">SQL store</legend>
      <label for="record" class="mb-1 block text-sm font-medium">Text record</label>
      <input
        id="record"
        name="record"
        bind:value={record}
        required
        class="mb-3 w-full rounded border border-gray-300 px-2 py-1.5"
      />
      <button
        id="add"
        type="submit"
        class="mr-2 rounded bg-blue-600 px-3 py-1.5 text-white hover:bg-blue-700 disabled:opacity-50"
      >Add record</button>
      <button
        id="list"
        type="button"
        onclick={() => void operateSql(false)}
        class="rounded border border-gray-300 px-3 py-1.5 hover:bg-gray-100 disabled:opacity-50"
      >List records</button>
    </fieldset>
  </form>
  <p
    id="sql-status"
    role="status"
    aria-live="polite"
    aria-atomic="true"
    data-error={String(sqlStatusError)}
    class={sqlStatusError ? "mb-4 font-bold" : "mb-4"}
  >{sqlStatus}</p>
  <div id="sql-result" aria-label="Committed SQL records" class="mb-8">
    {#if sqlResult.length === 0}
      <p>No records yet.</p>
    {:else}
      <ol class="list-decimal pl-6">
        {#each sqlResult as row, i (i)}
          <li>{row}</li>
        {/each}
      </ol>
    {/if}
  </div>

  <p class="text-sm text-gray-500">
    Storage belongs to this browser origin and may be cleared or evicted. It is
    not a backup.
  </p>
  <noscript>Enable JavaScript and reload to run this Wasm example.</noscript>
</main>
