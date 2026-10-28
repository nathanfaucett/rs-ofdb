/**
 * WebDriver async-script body. Runs against the served public controls, with fresh
 * driver-managed storage retained across the reload phase. The final argument is
 * WebDriver's completion callback; errors include the phase and visible status.
 * @param {"save"|"committed"|"reload"|"sql-save"|"sql-committed"|"sql-duplicate"|"reload-again"|"failure"} phase First WebDriver script argument.
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
        throw new Error(
          `Timed out: KV: ${status?.textContent}; SQL: ${document.querySelector("#sql-status")?.textContent}`,
        );
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
  await wait(() => {
    const controls = document.querySelector("#sql-controls");
    return controls !== null && !controls.disabled;
  });
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
  } else if (phase === "sql-save") {
    check(
      document.querySelector("#sql-result").textContent === "No records yet.",
      "Fresh SQL is not empty",
    );
    document.querySelector("#record").value = distinctive;
    document.querySelector("#record").focus();
  } else if (phase === "sql-committed") {
    await wait(
      () =>
        document.querySelector("#sql-status").textContent ===
        "Added and committed.",
    );
  } else if (phase === "sql-duplicate") {
    document.querySelector("#record").value = distinctive + " second";
    const form = document.querySelector("#sql-form");
    form.requestSubmit();
    check(
      document.querySelector("#sql-controls").disabled &&
        form.getAttribute("aria-busy") === "true",
      "SQL pending controls are not guarded",
    );
    form.requestSubmit();
    await wait(
      () =>
        document.querySelector("#sql-status").textContent ===
        "Added and committed.",
    );
  } else if (phase === "reload-again") {
    document.querySelector("#list").click();
    await wait(() => !document.querySelector("#sql-controls").disabled);
    check(
      document.querySelector("#sql-status").textContent ===
        "Listed committed records.",
      "Explicit SQL refresh failed",
    );
    const rows = document.querySelectorAll("#sql-result li");
    check(
      rows.length === 2 &&
        rows[0].textContent === distinctive &&
        rows[1].textContent === distinctive + " second",
      "SQL reload lost or duplicated literal data",
    );
    check(
      !document.querySelector("#sql-result img") && !window.unsafeMarkup,
      "SQL interpreted markup",
    );
    check(result.textContent === distinctive, "KV lost after SQL reload");
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
      const sqlStatus = document.querySelector("#sql-status");
      check(
        sqlStatus.textContent.startsWith("Error:") &&
          sqlStatus.textContent.includes("storage") &&
          sqlStatus.textContent.includes("reload") &&
          document.querySelector("#sql-controls").disabled,
        "SQL startup failure lacks recovery guidance or enables controls",
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
