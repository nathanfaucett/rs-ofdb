/**
 * Example-local UI glue, not a public JavaScript SDK. All database construction
 * and queries remain in Rust. Loading bindings inside the startup try/catch also
 * makes missing Wasm/build artifacts visible instead of leaving a frozen form.
 * @module app
 */
const controls = document.querySelector('#controls');
const form = document.querySelector('form:not(#sql-form)');
const key = document.querySelector('#key');
const value = document.querySelector('#value');
const status = document.querySelector('#status');
const result = document.querySelector('#result');

/** Show a text-only, accessible operation or error message.
 * @param {string} message Visible status.
 * @param {boolean} [error=false] Whether recovery is needed.
 * @returns {void}
 */
function report(message, error = false) {
  status.textContent = message;
  status.dataset.error = String(error);
}

/** Open persistent storage and install guarded native form actions.
 * @returns {Promise<void>} Resolves once initialization finishes or is reported.
 */
async function start() {
  controls.disabled = true;
  try {
    const { default: init, Kv } = await import('./pkg/ofdb_browser_example.js');
    await init();
    const kv = await Kv.open();

    /** Run one operation at a time, showing success only after its awaited result.
     * @param {boolean} save True for a committed write, false for a fresh read.
     * @returns {Promise<void>} Settles after the UI reports success or failure.
     */
    async function operate(save) {
      if (controls.disabled || !form.reportValidity()) return;
      const focused = document.activeElement;
      controls.disabled = true;
      form.setAttribute('aria-busy', 'true');
      result.textContent = '';
      report(save ? 'Saving…' : 'Retrieving…');
      try {
        if (save) {
          await kv.save(key.value, value.value);
          report('Saved and committed.');
        } else {
          const stored = await kv.retrieve(key.value);
          result.textContent = stored ?? '';
          report(stored === undefined ? 'Key not found.' : 'Retrieved committed value.');
        }
      } catch (error) {
        report(`Error: ${error.message || error}. Check browser storage permissions and available space; reload and retrieve before retrying a write.`, true);
      } finally {
        controls.disabled = false;
        form.setAttribute('aria-busy', 'false');
        focused?.focus();
      }
    }

    /** Submit the native form without navigation; Enter uses the same save path.
     * @param {SubmitEvent} event Native form submission.
     * @returns {void}
     */
    form.onsubmit = event => {
      event.preventDefault();
      void operate(true);
    };
    /** Retrieve the committed value through a fresh Rust Client read.
     * @returns {void}
     */
    document.querySelector('#retrieve').onclick = () => { void operate(false); };
    controls.disabled = false;
    await operate(false);
  } catch (error) {
    controls.disabled = true;
    report(`Error: ${error.message || error}. Check that the example is built and served over HTTP, allow browser storage, then reload. No memory fallback is used.`, true);
  }
}

await start();

/** Initialize SQL independently, preserving the usable KV section on SQL errors.
 * @returns {Promise<void>} Settles after initialization and a fresh committed read.
 */
async function startSql() {
  const form = document.querySelector('#sql-form');
  const controls = document.querySelector('#sql-controls');
  const status = document.querySelector('#sql-status');
  const result = document.querySelector('#sql-result');
  controls.disabled = true;
  try {
    const { Sql } = await import('./pkg/ofdb_browser_example.js');
    const sql = await Sql.open();
    /** Read persisted rows and render only text, including the empty state.
     * @returns {Promise<void>} Resolves after a fresh Client read and rendering.
     */
    async function list() {
      const records = await sql.list();
      result.replaceChildren();
      if (!records.length) result.textContent = 'No records yet.';
      else {
        const list = document.createElement('ol');
        for (const text of records) {
          const item = document.createElement('li');
          item.textContent = text;
          list.append(item);
        }
        result.append(list);
      }
    }
    /** Serialize UI operations; never retry a possibly committed write automatically.
     * @param {boolean} add Whether to insert the validated input before reading.
     * @returns {Promise<void>} Settles after committed results or actionable error.
     */
    async function operate(add) {
      if (controls.disabled || (add && !form.reportValidity())) return;
      const focused = document.activeElement;
      controls.disabled = true;
      form.setAttribute('aria-busy', 'true');
      status.dataset.error = 'false';
      status.textContent = add ? 'Adding…' : 'Listing…';
      try {
        if (add) await sql.add(document.querySelector('#record').value);
        await list();
        status.textContent = add ? 'Added and committed.' : 'Listed committed records.';
      } catch (error) {
        status.dataset.error = 'true';
        status.textContent = `Error: ${error.message || error}. Check browser storage permissions and available space; reload and list records before retrying a write.`;
      } finally {
        controls.disabled = false;
        form.setAttribute('aria-busy', 'false');
        focused?.focus();
      }
    }
    /** Use native Enter submission without navigation.
     * @param {SubmitEvent} event Native submission.
     * @returns {void}
     */
    form.onsubmit = event => { event.preventDefault(); void operate(true); };
    /** Refresh from committed storage.
     * @returns {void}
     */
    document.querySelector('#list').onclick = () => { void operate(false); };
    await list();
    status.textContent = 'Listed committed records.';
    controls.disabled = false;
  } catch (error) {
    status.dataset.error = 'true';
    status.textContent = `Error: ${error.message || error}. Check the Wasm build and browser storage permissions, then reload. No memory fallback is used.`;
  }
}

await startSql();
