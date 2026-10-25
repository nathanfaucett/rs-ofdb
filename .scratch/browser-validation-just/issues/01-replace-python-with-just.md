# 01: Replace Python browser orchestration with just

**What to build:** Contributors and CI run the existing browser validation suites through the project's just interface without Python or a replacement application-level runner. Preserve the validation contracts in the parent spec, complete the migration, and delete obsolete tooling in this ticket.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Discoverable, parameterized just recipes cover independent facade feature builds, browser and store-specific remote dependency isolation, browser Clippy, compiled public Rust quickstarts, browser acceptance, snapshot measurements, native regressions, and final validation gates.
- [ ] Browser acceptance continues to run the full Rust library tests through wasm-pack with sync enabled, covering Chromium and Firefox in Window and Dedicated Worker, with individual-cell and full-matrix selection. WebKit is not required.
- [ ] Existing driver/capabilities configuration, measurement runner selection, output selection, stack settings, and Cargo configuration behavior remain supported and documented. Arguments are handled safely, including paths containing spaces; invalid selections and nonpositive timeouts fail clearly.
- [ ] Commands have configurable finite timeouts covering compilation and execution. Timeouts and interruption clean up launched descendants with bounded escalation; missing tools, command failures, dependency-policy failures, and timeouts cannot be reported as success.
- [ ] Per-check logs, toolchain/revision context, elapsed time, exit status where available, and incremental machine-readable summaries remain available. Aggregate recipes exit nonzero when any required check fails, without a logging pipeline masking the command's status; safely independent checks retain useful evidence.
- [ ] Controlled checks through the public recipes verify matrix/feature selection, driver forwarding, safe argument handling, dependency rejection despite a successful Cargo exit, missing tooling, failure propagation, timeout descendant cleanup, and evidence output. Replace Python-specific tests without adding a testing runtime or generic runner framework.
- [ ] CI and current reproduction instructions invoke just and publish the replacement evidence. Historical execution records remain unchanged as historical evidence.
- [ ] Delete the Python matrix runner and its tests; retain no compatibility wrapper. Use the existing Bash shell and ordinary documented utilities, with small shell helpers only where necessary. Do not introduce Node tooling or a Rust xtask.
- [ ] Run the real feature checks and all four supported browser/context acceptance cells, exercise measurement and native recipes, and run applicable formatting, Clippy, feature-powerset, and CRAP final gates. Record actual results and existing failures/timeouts accurately without weakening checks or fixing unrelated problems.
