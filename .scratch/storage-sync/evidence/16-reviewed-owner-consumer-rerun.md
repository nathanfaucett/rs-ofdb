# Reviewed owner correction — consumer control GREEN, pair blockers remain

2026-10-12. User reports independent source approval of correction `cce094c`, with source-only malformed-fallback/custom-mutable-kernel limitations and no actionable regression. No owner main merge, source change, ticket closure or independent runtime-review claim.

## Executed provenance and evidence

Original owner root clean on `diagnostics/native-idp-stage-timing` at **`6ffb434cfbb744b4ea5cd7e818ee861fa94ad2b1`** both before and after all runs. Corrected source `cce094c`; reviewed diagnostics `b1d957d` remain separate from main. Consumer clean `f0be1aa8fc89ab0bcde8ce284892159e010a1beb` both before/after, unchanged source/harness/lockfile. Dependency sibling link resolves original owner root; compile output confirms corrected owner crates through that link.

Full literal commands, filters verified from source, exact raw hashes/statuses/progress and limitations: [consumer receipt](../../../../of/.worktrees/ticket08-full-nodes/.scratch/offline-first-nodes/evidence/08-reviewed-owner-rerun.md). Consumer receipt and new raw/argv/status files are ignored/uncommitted for parent integration, not contained in this owner documentation commit.

User granted exclusive build slot; no detected cargo/rustc build at preflight/postflight. Available memory16GiB preflight. Four commands launched sequentially using original shared `of/target`, standard profiles, offline/locked/jobs1,4G/zero swap/service900s/terminal910000ms. No cap/profile/deadline/cache/provider changes. Control same cli/stage-timing-enabled two-worker120s/HTTP10s; historical pair commands use no extra feature, two-worker360s/HTTP10s. No stage logger installed for pairs.

| Run / raw consumer log | Actual result |
| --- | --- |
| `08-native-stage-timing-run03.log` | Compile10m00s; **1passed/0failed,53.53s,exit0**. Extra four Clients, genuine refresh replay denial; restriction PUT2046.632302s client, inside unchanged10s. Ordinary shutdown completed. |
| `08-full-nodes-run09.log` | Compile4m47s; **0passed/1failed,328.94s,exit101**. A and B concurrent refresh winner/replay reached, both disconnected reopen and persisted consumed-grant/JWKS checks reached; A offline identity/policy/resource/user+client journey complete. B identity mutation/client issuance reached but its restriction PUT timed out; B resource journey incomplete. |
| `08-full-nodes-run10.log` | Compile0.51s; **0passed/1failed,361.70s,exit101**. Both local shared-grant redemption/replay denials reached; A target consumption/revocation and denials reached. Began IdP reconnect207.091537s; no reconciled marker before unchanged360s scenario bound. |
| `08-full-nodes-run11.log` | Compile0.40s; **0passed/1failed,258.41s,exit101**. A restriction PUT2047.611070s server and local/resource progress; B restriction PUT timed out before Management reconnect/retained-socket learned denial. |

No OOM or systemd900s timeout; result success/exit-code according to table. Reported256.0K MemoryPeak is implausibly low, not real RSS/headroom evidence. No B restriction204 in failing pair09/11; timeout neither proves no effect nor completed mutation. The360s timeout's test-runner completion361.70s reflects observed scheduling/cancellation completion, not a changed deadline.

Control enabled native interval:1512 catalog reconstruction records/4.025249s aggregate versus prior old-owner run022355/7.097790s; admission237 records/3.253190s aggregate/max1.090455s versus249/11.044561s/max4.058403s. These overlap with bodies/request stages and include background/shutdown work, not exclusive PUT or isolated CPU. Same control changed from failure to pass at reviewed corrected source; no universal deadline guarantee or attribution of remaining pair failure follows.

## Concrete blockers and next action

- Parent consumer diagnostic: `Pair::reconnect` (`full_node_tests.rs:841–868`) awaits endpoint DATA connection then `synchronize_security`, with no intervening marker. Add a safe stage distinction before another grants probe; output does not isolate which await stalled or justify a network/owner correction. Keep actual authenticated transport and360s bound.
- Joined-B cold Management restriction remains slow in pair09/11. Pair11 nested successful token/introspection/Device lookup intervals2.289289/6.534459/2.714359s sum11.538107s (not exclusive request/CPU). Existing pair runs provide no IdP/native stage breakdown. Parent should measure focused joined-B request, imported/two-signer/composed background distinctions, without changing permissions/conflict checks, caching, prewarming or10s deadline.
- Retain approved minimal owner correction and diagnostics separately from main. New partial offline/replay progress and single-control pass do not complete both offline journeys, reconciliation or learned retained-socket denial.

Session validation: exactly four bounded consumer tests above; source/status/argv/whitespace checks. No owner source/test change or further builds, powerset/CRAP/Clippy sweep. Complexity low for run/receipt, diagnostic attribution risk medium, priority remains local blockers. Exclusive Rust slot released with final report.
