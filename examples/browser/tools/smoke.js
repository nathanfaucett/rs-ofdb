/**
 * WebDriver async-script body. Runs against the served public controls, with fresh
 * driver-managed storage retained across the reload phase. The final argument is
 * WebDriver's completion callback; errors include the phase and visible status.
 * @param {"save"|"committed"|"reload"|"failure"} phase First WebDriver script argument.
 * @returns {Promise<void>} Settles after visible assertions; the caller forwards
 * completion or diagnostics through the WebDriver callback.
 */
async function smoke(phase) {
  const status = document.querySelector("#status");
  /** Wait for a visible UI condition, failing within 15 seconds.
   * @param {() => boolean} condition Observable state to await.
   * @returns {Promise<void>} Resolves only when the condition is true.
   */
  async function wait(condition) {
    const deadline = Date.now() + 15000;
    while (!condition()) {
      if (Date.now() > deadline)
        throw new Error(`Timed out: ${status?.textContent}`);
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  /** Assert a user-observable contract.
   * @param {boolean} condition Contract result.
   * @param {string} message Failure diagnostic.
   * @returns {void}
   */
  function check(condition, message) {
    if (!condition) throw new Error(message);
  }
  await wait(() => !document.querySelector("#controls").disabled);
  const key = document.querySelector("#key");
  const value = document.querySelector("#value");
  const result = document.querySelector("#result");
  const distinctive =
    '<img src=x onerror="window.unsafeMarkup=true"> ofdb; \' persistent ✓';
  key.value = "theme";
  if (phase === "save") {
    const source = await (await fetch("/app.js")).text();
    const AsyncFunction = Object.getPrototypeOf(smoke).constructor;
    new AsyncFunction(source);
    key.focus();
    check(
      document.activeElement === key,
      "Native key input cannot receive keyboard focus",
    );
    document.querySelector("#retrieve").click();
    await wait(() => status.textContent === "Key not found.");
    value.value = distinctive;
    key.focus();
    // The runner sends real Tab, Tab, Enter events from here.
  } else if (phase === "committed") {
    await wait(() => status.textContent === "Saved and committed.");
    check(
      !document.querySelector("#controls").disabled,
      "Save left controls disabled",
    );
  } else if (phase === "reload") {
    check(
      value.value === "",
      "Reload retained form input instead of fresh page state",
    );
    await wait(() => result.textContent === distinctive);
    document.querySelector("#retrieve").click();
    await wait(() => status.textContent === "Retrieved committed value.");
    check(
      result.textContent === distinctive,
      "Committed value did not survive reload",
    );
    check(
      !result.querySelector("img") && !window.unsafeMarkup,
      "Input interpreted as markup",
    );
  } else {
    // Browser-test-only denial: re-run the actual module with IndexedDB.open
    // denied, without adding failure injection hooks to the application.
    const original = indexedDB.open;
    indexedDB.open = () => {
      throw new DOMException("Storage denied by smoke check", "SecurityError");
    };
    try {
      await import(`/app.js?storage-denied=${Date.now()}`);
      await wait(() => status.textContent.startsWith("Error:"));
      check(
        status.textContent.includes("storage") &&
          status.textContent.includes("reload"),
        "Error lacks recovery guidance",
      );
      check(
        document.querySelector("#controls").disabled,
        "Failed startup enables controls",
      );
    } finally {
      indexedDB.open = original;
    }
  }
}

const done = arguments[arguments.length - 1];
smoke(arguments[0]).then(
  () => done("ok"),
  (error) => done(String(error.stack || error)),
);
