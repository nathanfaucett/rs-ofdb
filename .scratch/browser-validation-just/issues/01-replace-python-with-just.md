# 01: Replace Python browser orchestration with just

**What to build:** Contributors and CI run the existing browser validation suites through the project's just interface without Python or a replacement application-level runner. Preserve the validation contracts in the parent spec, complete the migration, and delete obsolete tooling in this ticket.

**Blocked by:** None (can start immediately).

**Status:** complete

- [x] Discoverable, parameterized just recipes cover independent facade feature builds, browser and store-specific remote dependency isolation, browser Clippy, compiled public Rust quickstarts, browser acceptance, snapshot measurements, native regressions, and final validation gates.
- [x] Browser acceptance continues to run the full Rust library tests through wasm-pack with sync enabled, covering Chromium and Firefox in Window and Dedicated Worker, with individual-cell and full-matrix selection. WebKit is not required.
- [x] Existing driver/capabilities configuration, measurement runner selection, output selection, stack settings, and Cargo configuration behavior remain supported and documented. Arguments are handled safely, including paths containing spaces; invalid selections and nonpositive timeouts fail clearly.
- [x] Commands have configurable finite timeouts covering compilation and execution. Timeouts and interruption clean up launched descendants with bounded escalation; missing tools, command failures, dependency-policy failures, and timeouts cannot be reported as success.
- [x] Per-check logs, toolchain/revision context, elapsed time, exit status where available, and incremental machine-readable summaries remain available. Aggregate recipes exit nonzero when any required check fails, without a logging pipeline masking the command's status; safely independent checks retain useful evidence.
- [x] Controlled checks through the public recipes verify matrix/feature selection, driver forwarding, safe argument handling, dependency rejection despite a successful Cargo exit, missing tooling, failure propagation, timeout descendant cleanup, and evidence output. Replace Python-specific tests without adding a testing runtime or generic runner framework.
- [x] CI and current reproduction instructions invoke just and publish the replacement evidence. Historical execution records remain unchanged as historical evidence.
- [x] Delete the Python matrix runner and its tests; retain no compatibility wrapper. Use the existing Bash shell and ordinary documented utilities, with small shell helpers only where necessary. Do not introduce Node tooling or a Rust xtask.
- [x] Run the real feature checks and all four supported browser/context acceptance cells, exercise measurement and native recipes, and run applicable formatting, Clippy, feature-powerset, and CRAP final gates. Record actual results and existing failures/timeouts accurately without weakening checks or fixing unrelated problems.

## Comments

### Implementation and validation — 2026-10-28

- **Review handoff, not closure:** implementation complete; coordinator to review
  and close. Worktree `/tmp/ofdb-browser-validation-just`, branch
  `implement/browser-validation-just-ticket`. Before edits, clean worktree HEAD,
  integration `implement/browser-validation-just`, and merge-base all matched
  `a46a273ad50f537a53b417e1b83c3f45ed131598`.
- **Complexity/risk/priority:** moderate orchestration complexity; highest priority
  is avoiding false success and orphaned processes. No database behavior, Rust
  dependency, storage, protocol, or synchronization changes. `git diff --exit-code
-- Cargo.toml Cargo.lock crates tests/browser/src` passed against that base.
- Recipes in `justfile` select all suites. A small sourced Bash helper owns only
  common process-group supervision, dependency policy and JSONL evidence; a bounded
  shell helper compiles the four current published Rust blocks. No competing public
  runner, generic framework, Node, Python replacement runtime or compatibility shim.
- Public recipes: `browser-check`, `browser-accept`, `browser-measure`,
  `browser-native`, `browser-final`, `browser-orchestration-test`. Positional args
  avoid just interpolation into shell source. Driver environment defaults,
  capabilities, runner selection, output paths, Cargo config discovery, independent
  features, native stack and browser budgets remain supported. Measurement uses
  a Cargo runner array to preserve whitespace in executable paths.
- GNU timeout/setsid bound builds and tests, TERM then KILL within five seconds.
  INT/TERM traps terminate the active group and escalate after one second;
  remaining inherited-group descendants are killed even after a leader exits.
  Raw logs, policy diagnostics, environment, exact argv/cwd/start time/status/
  exit code/whole-second elapsed time are retained. JSONL records publish after
  each check, including interruption. Failure does not skip independent checks.
  Deliberately detached/new-session descendants and supervisor SIGKILL are outside
  the documented shell cleanup guarantee. Linux/Bash/jq/coreutils/util-linux are
  documented prerequisites.
- **Controlled red/green:** first public matrix check failed because recipes did
  not exist. Added recipe coverage before removing Python. Expanded checks caught
  jq treating command options as its own options (fixed with `--`); controlled
  unavailable and signal tests were corrected to exercise a genuinely absent
  executable and terminal-style process-group signals, rather than accidental
  fallback to installed tools or background-inherited SIGINT ignoring.
- **Controlled suite passed:** all four full-library/context argv selections;
  focused cell and environment defaults; seven exact independent feature builds;
  both browser Clippy contexts and current quickstarts; literal spaces/metacharacters
  in paths; both driver and capabilities forwarding; all retained native dependency
  categories and both remote store policies (zero-exit Cargo trees still fail);
  permitted Tokio; invalid selection/nonpositive/nonnumeric budgets; actual exit 7;
  unavailable executable and measurement runner; incremental valid JSONL/logs;
  aggregate failures and later final gates; measurement config and opposite-driver
  clearing; timeout with TERM-resistant descendant and with resistant leader
  (KILL/137); INT and TERM cleanup with under-five-second response and no live
  descendant (Linux `/proc`, zombies excluded). The suite itself is bounded at
  180 seconds and uses temporary tool substitutes, no compilation or browsers.
- **CI/docs:** browser workflow installs pinned just 1.49.0 and shell utilities,
  invokes public recipes, constructs capabilities with jq, and uploads replacement
  artifacts with `always()`. Pinned action SHAs, read-only token, PR/manual triggers,
  independent matrix cells and environment-mediated matrix values are preserved.
  Current reproduction/harness instructions migrated; historical evidence below
  the dated acceptance heading is byte-for-byte unchanged. Deleted
  `scripts/browser_matrix.py`, `scripts/test_browser_matrix.py` and obsolete
  Python cache ignore only after replacement coverage passed.

### Real evidence and remaining limits

All paths below are local ignored artifacts under worktree
`target/browser-validation/`; these are local runs, **not remote Actions results**.
Rust **1.99.1**, wasm-pack **0.15.0**, fixture wasm-bindgen **0.2.128**, two Cargo
jobs, default **16 MiB** native thread stack and **240-second** browser test budget;
fixture optimized dev build with **16 MiB Wasm stack**. Explicit local capabilities
select installed Flatpak Chrome **156.0.8078.73** and Firefox **159.0**. Matching
ChromeDriver downloaded into ignored `target/browser-tools/`; cached GeckoDriver
and matching wasm-bindgen runner reused. No machine paths/binaries committed.

| Public recipe/run                                                                                              | Result                                                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `browser-check target/browser-validation/check 600`                                                            | **23/23 checks passed**, including seven independent facade builds, all graphs, Window/Worker Clippy and compiled current quickstarts                                                                       |
| `browser-accept all all target/browser-validation/browser 900`                                                 | **All four full-library sync cells passed**; Chrome Window **90 / 330 s**, Chrome Worker **87 / 236 s**, Firefox Window **90 / 170 s**, Firefox Worker **87 / 195 s** (seconds include compilation/startup) |
| `browser-measure all all target/browser-validation/measure 600 RUNNER CHROME_DRIVER GECKO_DRIVER`              | **1 filtered snapshot test passed in each cell**; explicit fixture Cargo config and opposite driver removed                                                                                                 |
| `browser-measure chrome window 'target/browser-validation/measure space' 600 '.../runner space' CHROME_DRIVER` | **Passed**, real executable and evidence paths containing spaces, 9 seconds                                                                                                                                 |
| `browser-native target/browser-validation/native 900`                                                          | **Nonzero**: protocols/ownership **5 passed**, KV **19 passed**, SQL **74 passed / 1 failed**, runtime **10 passed**, native Clippy passed                                                                  |
| `browser-native target/browser-validation/native-rerun 600`                                                    | **Same SQL failure reproduced**, other native checks still pass                                                                                                                                             |
| `browser-final target/browser-validation/final 240`                                                            | Workspace/fixture formatting passed; workspace Clippy timed out at **240 s**; powerset failed **exit 1 / 42 s** (underlying Cargo 101); CRAP timed out **240 s**; all independently attempted               |
| `timeout --kill-after=5 600 just clippy`                                                                       | **Passed** on bounded warmed retry, **34.36 s** Cargo wall time; `workspace-clippy-extended.log`                                                                                                            |

Snapshot observations, one fresh filtered instance per cell, **not benchmarks or
peak heap**: Chrome Window **3/33 ms**, Chrome Worker **5/41 ms**, Firefox Window
**4/20 ms**, Firefox Worker **2/19 ms**, for **100/1,000** records (**102,800 /
1,028,000** raw key+value bytes). Every sample's Wasm linear capacity was
**17,891,328 → 18,022,400** and **18,939,904 → 19,988,480** bytes respectively.
This is allocator capacity, not live allocations, RSS, JS/IDB memory or a safe
maximum size. The extra path-space run is separate from these four samples.

**Unchanged blockers, not waived:**

- SQL `candidate_outgoing_export_correlates_all_selected_rows_once` at
  `crates/sql/tests/scoped_sync.rs:1087` observes **2**, expects **1** outgoing
  observations. Reproduced on both native runs with unchanged base Rust sources
  and manifests. No runtime fix or weakened assertion. Native recipe correctly
  returns 1, records Cargo **101**, and still executes runtime/Clippy afterwards.
- Feature powerset stops at **8/1035**, `ofdb-btree --no-default-features --features
sharded`: eight previously documented missing `std`/`async_stream`/in-memory
  export/`ToString` errors. Remaining combinations did not complete.
- `just crap` reached coverage compilation for package **6/32** (`ofdb-value`,
  compiling Redb) before its **240 s** bound. No complete coverage/CRAP score.
- Native low-stack `sync_stack` is not part of this focused native suite; no
  default-stack SQL, WebKit, physical quota, power-loss or browser minimum claim.

Additional checks: `bash -n` for all three new scripts, actionlint **1.7.7** on the
workflow, and `git diff --check` passed. Process inspection found no live test
browser/driver/runner processes after real runs. No unrelated fixes made.

- **Additional isolated native reproduction:** with the same feature flags and
  stack/jobs, bounded `cargo test --test scoped_sync
candidate_outgoing_export_correlates_all_selected_rows_once -- --exact` also
  failed **exit 101**, **0 passed / 1 failed / 24 filtered**, at the same 2-versus-1
  assertion. Raw output: `target/browser-validation/native-isolated.log`.

### P2 review fix — PID-only SIGINT — 2026-10-28

- **Location/authority:** applied directly on integration branch
  `implement/browser-validation-just` in `/home/nathan/code/rust/aicacia/ofdb`,
  starting from clean `0ad0c72`. The original ticket worktree at
  `/tmp/ofdb-browser-validation-just` was not edited. At this handoff, status was
  ready-for-review; final review disposition is recorded below.
- **Complexity/risk/priority:** small test/documentation change; high priority is
  an honest cancellation/evidence contract. No production process-control changes,
  replacement public runner, generic framework, dependency or Rust changes.
- **Investigation:** installed `just --version` reports **1.49.0**. Its tagged
  [`src/signal_handler.rs`](https://github.com/casey/just/blob/1.49.0/src/signal_handler.rs)
  deliberately does not forward INT/HUP/QUIT, assuming terminal process-group
  delivery; it waits for the child. This prevents a recipe Bash trap from observing
  INT sent only to the outer public just PID. Source also forwards TERM, but the
  documented cancellation contract here requires group delivery for both signals.
  Adding a Bash trap cannot fix an unreceived signal; no polling workaround or
  competing launcher was added. This is a documented implementation limitation,
  not an amendment to requirements or a waiver of cleanup/failure reporting.
- **Red reproduction:** added a public `browser-accept chrome window` check with
  a hanging substitute command and TERM-resistant descendant, launched by
  `setsid env --default-signal=INT,TERM` (not background-inherited SIGINT ignoring).
  Sent INT only to the public just PID. A test expecting public exit 130 and an
  interrupted cell failed: the cell reached its **2-second timeout**, recorded
  `timeout`, and the public invocation returned **1**, not 130. Existing group
  cancellation tests passed before that new assertion failed.
- **Regression/green:** retain separate PID-only tests for the hanging command
  and a command held until an explicit release file. After PID-only INT both
  public PID and controlled command remain live, with only the four completed
  prerequisite records. Hanging case then records **timeout / 124**, public
  exit **1**; released-success case records **passed / 0**, public exit **130**.
  Each completes within the test's **under-eight-second** bound and leaves no live
  recorded command/descendant (`/proc`, zombies excluded). Thus passed per-check
  evidence is explicitly not treated as whole-run success.
- **Supported cancellation:** INT or TERM to the process group containing just
  **and recipe Bash**, including terminal foreground Ctrl-C. The recipe terminates
  its separately supervised active command group, escalates to KILL after one
  second, records `interrupted`, and returns nonzero. Existing controlled group
  tests still require **under-five-second** completion and no live TERM-resistant
  descendant. Per-command timeouts (including compilation) and their five-second
  escalation remain unchanged even for unsupported PID-only INT; independent
  checks may continue, so PID-only INT is not prompt cancellation. Detached/new-
  session descendants and supervisor SIGKILL remain outside the guarantee.
- **Docs/spec:** corrected the broad interruption wording, documented the exact
  limitation and status precedence, and supplied a dedicated-group Bash launch /
  cancellation example. Whole-run success requires **zero public just status AND
  passing required check records**. Earlier implementation evidence above tested
  group signals only and must not be read as a PID-only INT guarantee.
- **Validation:** `just browser-orchestration-test` passed twice after the red
  reproduction, including both new PID-only cases, existing timeout/KILL and group
  INT/TERM cleanup, and all matrix/policy/failure/evidence checks. Final run output
  is local ignored `target/browser-validation-review.log` in this integration
  checkout (not a remote Actions result). `bash -n` on all three shell scripts,
  `just --show browser-accept`, and `git diff --check` passed.
  `git diff --exit-code -- Cargo.toml Cargo.lock crates tests/browser/src` passed.
  Real browser/Rust/Clippy/powerset/CRAP gates were not rerun for this shell-test and
  documentation-only fix; previously recorded unrelated Rust blockers remain
  unchanged and are not waived.

### Final review disposition and evidence preservation — 2026-10-28

- **Disposition:** reviewer confirms the runtime finding is addressed and the
  controlled tests pass. Completed the requested documentation correction:
  restored the original normative timeout/interruption bullet in spec.md and the
  matching ticket criterion; moved just's signal-delivery limitation into a
  separate implementation note. Review acceptance is not user approval to amend
  requirements. The added PID-only and existing group-cleanup tests are unchanged.
- **Completion:** ticket and spec status are complete for the orchestration
  migration and review fix. This is not a claim that every real validation gate
  passed, nor a waiver of the original requirements. PID-only SIGINT remains a
  documented just limitation: prompt cancellation needs process-group delivery;
  finite per-command timeout cleanup remains enforced, and per-check `passed`
  evidence alone cannot establish whole-run success.
- **Preserved original evidence:** copied from
  `/tmp/ofdb-browser-validation-just/target/browser-validation/` into this
  integration checkout's ignored `target/browser-validation-migration-evidence/`.
  Preserved relative paths and raw bytes for **98 files / 236,065 bytes**, selecting
  only `.log`, `.json`, and `.jsonl` logs, summaries, and context. No build artifacts
  were copied. `diff -rq` between source and destination passed; `git check-ignore`
  confirms the destination is ignored. The original source and all worktrees are
  retained. Historical evidence paths above resolve beneath this preserved root
  in the integration checkout; the separate review suite output remains at
  `target/browser-validation-review.log`.
- **Validation limits:** original real feature checks passed **23/23**, all four
  browser acceptance cells and measurement cells passed; native SQL still has the
  recorded 2-versus-1 assertion failure, feature powerset stops at **8/1035** on
  pre-existing sharded errors, and CRAP did not complete within **240 seconds**.
  Workspace Clippy passed its bounded warmed retry after the earlier timeout.
  These are local results, not remote Actions results; closure does not turn
  failures/timeouts into passes. No unrelated Rust changes were made.
- **Doc-correction validation:** `bash -n` for all three scripts,
  `git diff --check`, and unchanged-script/justfile/Rust-source diff checks against
  review-fix commit `bd35678627363696a04bdfea840e2d871583e8db` passed. No controlled,
  browser, or Rust suites were rerun for this documentation-only correction; the
  two prior green controlled runs and reviewer confirmation remain the test
  evidence.
