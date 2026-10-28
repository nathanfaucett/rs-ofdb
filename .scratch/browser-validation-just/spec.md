# Browser validation through just

Status: ready-for-agent

## Problem Statement

Browser validation currently uses a Python matrix runner and Python-specific runner tests alongside Rust and JavaScript. Python works, but introduces another language and tooling surface to maintain when the project already uses just for development tasks.

Maintainers need to retain the existing validation coverage and trustworthy failure reporting without maintaining a separate application-level orchestration runner.

## Solution

Make the existing just task interface the authoritative entry point for browser validation. Parameterized recipes and the existing Bash shell orchestrate the Rust tests, Cargo checks, wasm-pack browser execution, measurements, and native regressions. Remove the Python implementation and its tests after equivalent externally observable orchestration checks are in place.

Local developers and CI use the same recipes. The migration must not reduce coverage or disguise incomplete validation as success.

## User Stories

1. As a maintainer, I want browser validation available through just, so that I use the project's existing task interface.
2. As a contributor, I want to run validation without Python, so that I do not maintain or install another language for this tooling.
3. As a contributor, I want discoverable recipes and prerequisite instructions, so that I can reproduce browser checks from a clean checkout.
4. As a library maintainer, I want independent minimal-feature builds for each facade, so that feature unification cannot hide unsupported combinations.
5. As a library maintainer, I want browser dependency checks, so that native storage, filesystem, and networking implementations do not enter supported browser builds.
6. As a library maintainer, I want store-specific remote dependency checks, so that remote-only clients remain isolated from embedded engines and sync implementations.
7. As a browser developer, I want Chromium and Firefox execution, so that both supported browser families remain covered.
8. As a browser developer, I want Window and Dedicated Worker execution, so that both supported contexts remain covered.
9. As a contributor, I want to select one browser and context or the full matrix, so that focused validation stays affordable.
10. As a contributor, I want explicit browser driver and capabilities configuration, so that nonstandard installations remain usable.
11. As a maintainer, I want snapshot measurements runnable through just, so that documented storage costs remain reproducible.
12. As a maintainer, I want native ownership, protocol, storage, and sync regressions retained, so that browser changes do not compromise native support.
13. As a documentation author, I want the public Rust quickstarts compiled, so that published API examples do not silently drift.
14. As a contributor, I want invalid options and missing prerequisites to fail clearly, so that configuration mistakes are not mistaken for passing tests.
15. As a CI operator, I want configurable finite command timeouts and child-process cleanup, so that stalled builds and tests do not leave work running indefinitely.
16. As a CI operator, I want per-check logs and a machine-readable summary, so that failures and partial runs remain diagnosable.
17. As a maintainer, I want nonzero command status and forbidden dependencies to fail the suite, so that successful output capture cannot hide a failure.
18. As a reviewer, I want controlled tests of orchestration behavior, so that changing the task interface does not require launching every browser to detect a reporting bug.
19. As a contributor, I want CI and current documentation to invoke the same recipes, so that there is no second maintained workflow.
20. As a maintainer, I want obsolete Python files deleted, so that the migration actually reduces the project's maintenance surface.

## Implementation Decisions

- Use the existing just task interface and configured Bash shell. Do not introduce a Node runner, Python replacement runner, Rust xtask, or generic task framework.
- Keep orchestration in parameterized recipes. Small shell helpers are acceptable only where necessary to avoid duplicated process-control or reporting logic; they must not become a competing public runner.
- Provide discoverable entry points for feature/dependency checks, browser acceptance, snapshot measurement, native regressions, final gates, and controlled orchestration verification. Exact recipe spelling is an implementation detail.
- Preserve independent KV feature checks for indexeddb and indexeddb plus sync. Preserve SQL checks for indexeddb alone, with SQL text, with sync, with both, and with explicitly enabled native staging excluded from the browser dependency graph.
- Preserve browser fixture dependency isolation, Window and Worker Clippy checks, and compilation of public Rust quickstarts. Do not retain Python merely to extract documentation examples; use existing shell tools or another minimal non-Python approach and continue checking the published examples rather than a stale copy.
- Preserve store-specific remote dependency isolation and the current forbidden browser dependency categories. A dependency-policy failure is a failed check even if Cargo exits successfully.
- Browser acceptance continues to execute the complete Rust browser library tests through wasm-pack with sync enabled. Worker selection must change the compiled execution context, not merely relabel the result.
- Chromium and Firefox, each in Window and Dedicated Worker, are the supported matrix. Allow focused and full selections. WebKit is not required.
- Retain configurable driver locations, browser capabilities, measurement runner selection, output destination, and positive per-command timeout budgets. Handle arguments and paths safely without evaluating caller-supplied shell code.
- Preserve the existing supported SQL stack configuration, Cargo configuration discovery behavior, test time budgets, and native stack settings. Do not claim default-stack support as a side effect of changing orchestration.
- Bound compilation as well as execution. A timeout must terminate the launched command and its descendants with bounded escalation; timeout, interruption, and unavailable tooling must never become success. Prompt cancellation requires INT/TERM to the process group containing just and the recipe Bash. Review accepted documenting just 1.49.0's non-forwarding of PID-only SIGINT rather than introducing a replacement public runner: per-command timeout cleanup must remain bounded, and whole-run success requires both zero public exit status and passing required check records.
- Use ordinary shell/system utilities where sufficient and document any required utility, including its supported platform assumptions. Do not add an npm environment or bespoke server to implement process control.
- Record toolchain/revision context, command identity, output, completion status, exit code where available, and elapsed time. Preserve per-check logs and a machine-readable summary without requiring the old Python command-line interface or exact JSON shape. Publish completed evidence incrementally so failures and interruptions leave useful artifacts.
- Aggregate recipes return nonzero when any required check fails. Where independent checks can still run safely, retain their evidence instead of losing it after the first failure. Prevent a successful logger or pipeline stage from masking the actual command status.
- Update browser CI to install and invoke just and publish the replacement evidence. Update current reproduction and harness instructions. Historical execution records remain historical evidence, not executable instructions to rewrite as if the new runner produced them.
- Delete the Python matrix runner and Python-specific tests once replacement coverage is in place. Do not retain compatibility wrappers.

## Testing Decisions

- The user confirmed the highest practical seam: invoke the public just recipes and observe their behavior, not private shell function implementation.
- Use controlled commands or temporary tool substitutes to verify the selected command matrix, feature isolation, driver forwarding, safe path handling, positive timeout validation, missing tooling, command failure, dependency-policy rejection, logging, summaries, and aggregate exit status.
- Include a timeout check that launches a descendant process and verifies cleanup, not merely the wrapper's return code. Verify failing commands retain their actual exit status even when output is logged. Test supported process-group INT/TERM cleanup and PID-only SIGINT separately, restoring default signal dispositions before launch. Cover both a hanging command (bounded timeout cleanup) and controlled successful completion (per-check passed evidence is not whole-run success) for the documented just limitation.
- Replace Python runner tests with checks executable through just using the existing shell/Rust tooling as appropriate. No new testing runtime is required.
- Use the current automation tests as prior art for dependency-policy cases, independent facade builds, full-library browser arguments, failure/unavailable/timeout outcomes, and successful Cargo exits with forbidden dependencies.
- Execute the real feature checks and all four supported browser/context acceptance cells after the controlled checks pass. Retain snapshot measurement and native regression reproduction.
- Continue using existing Rust behavioral tests at the public Database, Client, and Transaction seam; do not translate them into shell or JavaScript tests.
- Run formatting, Clippy, the feature-powerset gate, and CRAP as applicable final validation. Report existing failures, timeouts, or unavailable tools accurately; do not weaken gates or fix unrelated failures to make the migration appear green.

## Out of Scope

- Changing database storage, transaction, reconciliation, or synchronization behavior.
- Rewriting Rust browser tests in another language.
- Adding Playwright, a TypeScript test suite, an npm package, or an xtask solely for orchestration.
- Adding runnable browser application examples; those have a separate spec.
- WebKit, additional browser contexts, Windows shell portability, or new minimum browser guarantees.
- Fixing pre-existing sharded feature errors, SQL stack requirements, native low-stack failures, or unrelated CRAP performance problems.
- Preserving the Python command-line interface or rewriting historical validation evidence.

## Further Notes

The migration is primarily a maintenance simplification, but incorrect failure or process handling could create false validation confidence. Complexity is moderate; reporting and process-lifecycle risk are the highest-priority concerns. Preserve those contracts before deleting the old runner.

This spec is independent of the browser examples spec. The examples can use just without waiting for this migration, and this migration does not depend on an example application.

The user confirmed the proposed scope and test seam before publication. No runtime implementation, ticket decomposition, or commits are part of publishing this spec.
