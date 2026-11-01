/**
 * Persistent browser KV and SQL example.
 *
 * All application logic lives here in TypeScript: the schema, the queries, the
 * bound parameters, the row shaping and the DOM. Rust is a generic engine
 * bridge that opens IndexedDB stores and executes SQL text.
 */
import { KvStore, SqlStore } from "./db.js";

/** Look up one required element.
 * @param selector CSS selector.
 * @returns The matched element.
 */
function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`missing element: ${selector}`);
  return element;
}

/** Describe a thrown value for display. */
function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Open persistent storage and install guarded native form actions. */
async function startKv(): Promise<void> {
  const status = document.querySelector<HTMLElement>("#status");

  /** Show a text-only, accessible operation or error message.
   * @param text Visible status.
   * @param error Whether recovery is needed.
   */
  function report(text: string, error = false): void {
    if (!status) return;
    status.textContent = text;
    status.dataset.error = String(error);
  }

  try {
    const controls = required<HTMLFieldSetElement>("#controls");
    const form = required<HTMLFormElement>("form:not(#sql-form)");
    const key = required<HTMLInputElement>("#key");
    const value = required<HTMLTextAreaElement>("#value");
    const result = required<HTMLElement>("#result");
    const retrieve = required<HTMLButtonElement>("#retrieve");
    const kv = await KvStore.open("ofdb-browser-example-kv");

    /** Run one operation at a time, showing success only after its awaited result.
     * @param save True for a committed write, false for a fresh read.
     */
    async function operate(save: boolean): Promise<void> {
      if (controls.disabled || !form.reportValidity()) return;
      const focused = document.activeElement;
      controls.disabled = true;
      form.setAttribute("aria-busy", "true");
      result.textContent = "";
      report(save ? "Saving…" : "Retrieving…");
      try {
        if (save) {
          await kv.set(key.value, value.value);
          report("Saved and committed.");
        } else {
          const stored = await kv.get(key.value);
          if (stored === undefined) {
            report("Key not found.");
          } else {
            result.textContent =
              typeof stored === "string" ? stored : JSON.stringify(stored);
            report("Retrieved committed value.");
          }
        }
      } catch (error) {
        report(
          `Error: ${message(error)}. Check browser storage permissions and available space; reload and retrieve before retrying a write.`,
          true,
        );
      } finally {
        controls.disabled = false;
        form.setAttribute("aria-busy", "false");
        if (focused instanceof HTMLElement) focused.focus();
      }
    }

    form.onsubmit = (event) => {
      event.preventDefault();
      void operate(true);
    };
    retrieve.onclick = () => {
      void operate(false);
    };
    controls.disabled = false;
    await operate(false);
  } catch (error) {
    const controls = document.querySelector<HTMLFieldSetElement>("#controls");
    if (controls) controls.disabled = true;
    report(
      `Error: ${message(error)}. Check that the example is built and served over HTTP, allow browser storage, then reload. No memory fallback is used.`,
      true,
    );
  }
}

/** Initialize SQL independently, preserving the usable KV section on SQL errors. */
async function startSql(): Promise<void> {
  const sqlStatus = document.querySelector<HTMLElement>("#sql-status");

  try {
    const sqlForm = required<HTMLFormElement>("#sql-form");
    const sqlControls = required<HTMLFieldSetElement>("#sql-controls");
    const sqlResult = required<HTMLElement>("#sql-result");
    const record = required<HTMLInputElement>("#record");
    const listButton = required<HTMLButtonElement>("#list");
    const sql = await SqlStore.open("ofdb-browser-example-sql");

    const schema =
      "CREATE TABLE IF NOT EXISTS records (id UUID PRIMARY KEY, text TEXT)";
    const insert = "INSERT INTO records (text) VALUES ($1)";
    const select = "SELECT text FROM records ORDER BY id";
    await sql.execute(schema);

    /** Read persisted rows and render only text, including the empty state. */
    async function list(): Promise<void> {
      const results = await sql.execute(select);
      const rows = results.flatMap((result) => result.rows);
      sqlResult.replaceChildren();
      if (!rows.length) {
        sqlResult.textContent = "No records yet.";
        return;
      }
      const listElement = document.createElement("ol");
      for (const row of rows) {
        const item = document.createElement("li");
        const first = row[0];
        item.textContent =
          typeof first === "string" ? first : JSON.stringify(first);
        listElement.append(item);
      }
      sqlResult.append(listElement);
    }

    /** Serialize UI operations; never retry a possibly committed write automatically.
     * @param add Whether to insert the validated input before reading.
     */
    async function operate(add: boolean): Promise<void> {
      if (sqlControls.disabled || (add && !sqlForm.reportValidity())) return;
      const focused = document.activeElement;
      sqlControls.disabled = true;
      sqlForm.setAttribute("aria-busy", "true");
      if (sqlStatus) sqlStatus.dataset.error = "false";
      if (sqlStatus) sqlStatus.textContent = add ? "Adding…" : "Listing…";
      try {
        if (add) await sql.execute(insert, [record.value]);
        await list();
        if (sqlStatus)
          sqlStatus.textContent = add
            ? "Added and committed."
            : "Listed committed records.";
      } catch (error) {
        if (sqlStatus) sqlStatus.dataset.error = "true";
        if (sqlStatus) {
          sqlStatus.textContent = `Error: ${message(error)}. Check browser storage permissions and available space; reload and list records before retrying a write.`;
        }
      } finally {
        sqlControls.disabled = false;
        sqlForm.setAttribute("aria-busy", "false");
        if (focused instanceof HTMLElement) focused.focus();
      }
    }

    sqlForm.onsubmit = (event) => {
      event.preventDefault();
      void operate(true);
    };
    listButton.onclick = () => {
      void operate(false);
    };
    await list();
    if (sqlStatus) sqlStatus.textContent = "Listed committed records.";
    sqlControls.disabled = false;
  } catch (error) {
    const sqlControls =
      document.querySelector<HTMLFieldSetElement>("#sql-controls");
    if (sqlControls) sqlControls.disabled = true;
    if (sqlStatus) sqlStatus.dataset.error = "true";
    if (sqlStatus) {
      sqlStatus.textContent = `Error: ${message(error)}. Check the Wasm build and browser storage permissions, then reload. No memory fallback is used.`;
    }
  }
}

void startKv();
void startSql();
