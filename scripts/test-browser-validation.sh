#!/usr/bin/env bash
set -euo pipefail
tmp=$(mktemp -d)
trap 'code=$?; if [[ -n ${parent:-} ]]; then kill -TERM -- "-$parent" 2>/dev/null || true; wait "$parent" 2>/dev/null || true; fi; if [[ $code != 0 ]]; then cat "$tmp/run.log"; echo "failed at $BASH_COMMAND"; fi; rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/repo/scripts" "$tmp/repo/docs" "$tmp/repo/tests/browser"
cp justfile "$tmp/repo/"
cp scripts/*.sh "$tmp/repo/scripts/"
cp docs/browser.md "$tmp/repo/docs/"
cp tests/browser/Cargo.lock "$tmp/repo/tests/browser/"
cat > "$tmp/bin/cargo" <<'TOOL'
#!/usr/bin/env bash
printf '<%s>\n' "$@"
printf 'caps=%s\nrunner=%s\nchrome=%s\nfirefox=%s\n' "${WASM_BINDGEN_TEST_WEBDRIVER_JSON:-}" "${CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER:-}" "${CHROMEDRIVER:-}" "${GECKODRIVER:-}"
case ${MODE:-} in
    fail) [[ $1 != check ]] || exit 7 ;;
    gates) [[ $1 != clippy && ! ( $1 == hack && ${2:-} == test ) ]] || exit 9 ;;
    policy) if [[ $1 == tree ]]; then printf '%s v1.0.0\n' "$PACKAGE"; fi ;;
esac
TOOL
cat > "$tmp/bin/wasm-pack" <<'TOOL'
#!/usr/bin/env bash
printf '<%s>\n' "$@"
printf 'caps=%s\n' "${WASM_BINDGEN_TEST_WEBDRIVER_JSON:-}"
[[ $1 != --version ]] || exit 0
case ${MODE:-} in
    fail) exit 7 ;;
    missing) exit 127 ;;
    release)
        echo "$BASHPID" > "$CHILD_PID"
        until [[ -f $RELEASE ]]; do sleep 0.05; done ;;
    hang|stubborn)
        [[ $MODE != stubborn ]] || trap '' TERM
        bash -c 'trap "" TERM; echo "$BASHPID" > "$CHILD_PID"; exec sleep 90' &
        wait ;;
esac
TOOL
printf '#!/usr/bin/env bash\nprintf "controlled-revision\\n"\n' > "$tmp/bin/git"
chmod +x "$tmp/bin/"*
export PATH="$tmp/bin:$PATH"
recipe=("$(command -v just)" --justfile "$tmp/repo/justfile")
export WASM_BINDGEN_TEST_WEBDRIVER_JSON="$tmp/caps with spaces.json"
"${recipe[@]}" browser-accept all all "$tmp/matrix space" 5 'driver with spaces;$(false)' 'firefox driver' > "$tmp/run.log" 2>&1
jq -se 'map(select(.name | test("^(chrome|firefox)-"))) | length == 4 and all(.status == "passed")' "$tmp/matrix space/results.jsonl" >/dev/null
for browser in chrome firefox; do
    for context in window worker; do
        log="$tmp/matrix space/$browser-$context.log"
        grep -Fx "<--$browser>" "$log" >/dev/null
        grep -Fx '<--lib>' "$log" >/dev/null
        grep -Fx "caps=$WASM_BINDGEN_TEST_WEBDRIVER_JSON" "$log" >/dev/null
        feature=sync; [[ $context != worker ]] || feature=worker,sync
        grep -Fx "<$feature>" "$log" >/dev/null
    done
done
grep -Fx '<driver with spaces;$(false)>' "$tmp/matrix space/chrome-window.log" >/dev/null
grep -Fx '<firefox driver>' "$tmp/matrix space/firefox-window.log" >/dev/null
echo 'PASS: full library matrix, contexts, literal paths, drivers and capabilities'

"${recipe[@]}" browser-check "$tmp/check" 5 > "$tmp/run.log" 2>&1
jq -se 'length == 23 and all(.status == "passed")' "$tmp/check/results.jsonl" >/dev/null
for pair in 'ofdb-kv indexeddb' 'ofdb-kv indexeddb,sync' 'ofdb-sql indexeddb' 'ofdb-sql indexeddb,sql' 'ofdb-sql indexeddb,sync' 'ofdb-sql indexeddb,sql,sync' 'ofdb-sql indexeddb,sql,sync,native-stage'; do
    read -r package feature <<< "$pair"
    jq -se --arg name "$package-${feature//,/-}" --arg package "$package" --arg feature "$feature" 'any(.[]; .name == $name and .command == ["cargo","check","--locked","-p",$package,"--no-default-features","--features",$feature,"--lib","--target","wasm32-unknown-unknown"])' "$tmp/check/results.jsonl" >/dev/null
done
cmp <(awk '/^```rust$/ {r=1;next} /^```$/ {r=0} r' docs/browser.md) "$tmp/repo/target/browser-quickstarts/src/quickstarts.rs"
echo 'PASS: seven independent features, graphs, both Clippy contexts, current quickstarts'

for package in redb ofdb-btree-redb ofdb-sql-engine-redb tempfile mio socket2 fs2 fd-lock rustix libc tokio-stream hyper hyper-util tower tonic tonic-prost iroh-net ofdb-resource ofdb-sql-client ofdb-kv-client ofdb-sql-server ofdb-kv-server ofdb-sql-proto ofdb-kv-proto; do
    if MODE=policy PACKAGE="$package" "${recipe[@]}" browser-check "$tmp/policy" 5 > "$tmp/run.log" 2>&1; then echo "accepted forbidden $package" >&2; exit 1; fi
    jq -se 'any(.[]; .name == "fixture-graph" and .exit_code == 0 and .status == "failed")' "$tmp/policy/results.jsonl" >/dev/null
done
for package in redb automerge ofdb-btree ofdb-sql-engine ofdb-kv-store ofdb-sql-sync ofdb-kv-sync ofdb-sync-runtime; do
    if MODE=policy PACKAGE="$package" "${recipe[@]}" browser-check "$tmp/remote" 5 > "$tmp/run.log" 2>&1; then echo "accepted remote $package" >&2; exit 1; fi
    for store in sql kv; do
        jq -se --arg name "ofdb-$store-remote-graph" 'any(.[]; .name == $name and .exit_code == 0 and .status == "failed")' "$tmp/remote/results.jsonl" >/dev/null
    done
done
MODE=policy PACKAGE=tokio "${recipe[@]}" browser-check "$tmp/allowed" 5 > "$tmp/run.log" 2>&1
echo 'PASS: native/browser and store-specific remote policies reject zero-exit trees; Tokio allowed'

for value in 0 -1 text 0.5; do
    if "${recipe[@]}" browser-accept chrome window "$tmp/invalid" "$value" > "$tmp/run.log" 2>&1; then exit 1; fi
    grep 'timeout must be positive' "$tmp/run.log" >/dev/null
done
for selection in 'safari window' 'chrome shared'; do
    read -r browser context <<< "$selection"
    if "${recipe[@]}" browser-accept "$browser" "$context" "$tmp/invalid" 5 > "$tmp/run.log" 2>&1; then exit 1; fi
done
for mode in fail missing; do
    if MODE=$mode "${recipe[@]}" browser-accept all all "$tmp/$mode" 5 > "$tmp/run.log" 2>&1; then exit 1; fi
    status=failed; code=7
    [[ $mode != missing ]] || { status=unavailable; code=127; }
    jq -se --arg status "$status" --argjson code "$code" 'map(select(.name | test("^(chrome|firefox)-"))) | length == 4 and all(.status == $status and .exit_code == $code)' "$tmp/$mode/results.jsonl" >/dev/null
done
# An actually absent executable, not only a substitute returning 127.
mv "$tmp/bin/wasm-pack" "$tmp/bin/saved"
if PATH="$tmp/bin:/usr/bin:/bin" "${recipe[@]}" browser-accept chrome window "$tmp/absent" 5 > "$tmp/run.log" 2>&1; then exit 1; fi
jq -se 'any(.[]; .name == "chrome-window" and .status == "unavailable")' "$tmp/absent/results.jsonl" >/dev/null
mv "$tmp/bin/saved" "$tmp/bin/wasm-pack"
echo 'PASS: invalid selections/budgets, unavailable tools, actual exit 7 and aggregate failures'

export CHILD_PID="$tmp/child.pid"
not_live() {
    local pid
    pid=$(cat "$CHILD_PID")
    [[ ! -f /proc/$pid/stat ]] || [[ $(awk '{print $3}' "/proc/$pid/stat") == Z ]]
}
if MODE=hang "${recipe[@]}" browser-accept chrome window "$tmp/timeout" 1 > "$tmp/run.log" 2>&1; then exit 1; fi
jq -se 'any(.[]; .name == "chrome-window" and .status == "timeout" and (.exit_code == 124 or .exit_code == 137))' "$tmp/timeout/results.jsonl" >/dev/null
not_live
if MODE=stubborn "${recipe[@]}" browser-accept chrome window "$tmp/escalation" 1 > "$tmp/run.log" 2>&1; then exit 1; fi
jq -se 'any(.[]; .name == "chrome-window" and .status == "timeout" and .exit_code == 137)' "$tmp/escalation/results.jsonl" >/dev/null
not_live
for signal
 in TERM INT; do
    rm -f "$CHILD_PID"
    setsid env --default-signal=INT,TERM MODE=hang "${recipe[@]}" browser-accept chrome window "$tmp/interrupt-$signal" 15 > "$tmp/run.log" 2>&1 &
    parent=$!
    for ((i=0; i<100; i++)); do [[ ! -s $CHILD_PID ]] || break; sleep 0.05; done
    # Cargo/rustc/revision and wasm-pack version must already be published while the cell runs.
    jq -se 'length == 4 and all(.status == "passed")' "$tmp/interrupt-$signal/results.jsonl" >/dev/null
    interrupted_at=$SECONDS
    kill -"$signal" -- "-$parent"
    result=0; wait "$parent" || result=$?
    [[ $((SECONDS - interrupted_at)) -lt 5 ]]
    parent=''
    [[ $result != 0 ]]
    jq -se 'any(.[]; .name == "chrome-window" and .status == "interrupted")' "$tmp/interrupt-$signal/results.jsonl" >/dev/null
    not_live
done
echo 'PASS: timeout and process-group INT/TERM kill TERM-resistant descendants; incremental interruption evidence'

# Reset background-inherited signal dispositions before exercising the public just PID.
export RELEASE="$tmp/release"
for mode in hang release; do
    rm -f "$CHILD_PID" "$RELEASE"
    setsid env --default-signal=INT,TERM MODE=$mode "${recipe[@]}" browser-accept chrome window "$tmp/pid-int-$mode" 2 > "$tmp/run.log" 2>&1 &
    parent=$!
    for ((i=0; i<100; i++)); do [[ ! -s $CHILD_PID ]] || break; sleep 0.05; done
    [[ -s $CHILD_PID ]]
    jq -se 'length == 4 and all(.status == "passed")' "$tmp/pid-int-$mode/results.jsonl" >/dev/null
    interrupted_at=$SECONDS
    kill -INT "$parent"
    sleep 0.2
    kill -0 "$parent"
    ! not_live
    jq -se 'length == 4' "$tmp/pid-int-$mode/results.jsonl" >/dev/null
    [[ $mode != release ]] || touch "$RELEASE"
    result=0; wait "$parent" || result=$?
    [[ $((SECONDS - interrupted_at)) -lt 8 ]]
    parent=''
    if [[ $mode == hang ]]; then
        [[ $result == 1 ]]
        jq -se '.[-1] | .name == "chrome-window" and .status == "timeout" and .exit_code == 124' "$tmp/pid-int-$mode/results.jsonl" >/dev/null
    else
        [[ $result == 130 ]]
        jq -se '.[-1] | .name == "chrome-window" and .status == "passed" and .exit_code == 0' "$tmp/pid-int-$mode/results.jsonl" >/dev/null
    fi
    not_live
    echo "PASS: PID-only INT is not relayed ($mode); public exit $result; bounded completion and cleanup"
done

cp "$tmp/bin/wasm-pack" "$tmp/bin/runner space"
cp "$tmp/bin/wasm-pack" "$tmp/bin/chrome space"
cp "$tmp/bin/wasm-pack" "$tmp/bin/firefox space"
CHROMEDRIVER='inherited chrome' GECKODRIVER='inherited firefox' "${recipe[@]}" browser-measure all all "$tmp/measure" 5 'runner space' 'chrome space' 'firefox space' > "$tmp/run.log" 2>&1
jq -se 'map(select(.name | endswith("-snapshot"))) | length == 4 and all(.status == "passed")' "$tmp/measure/results.jsonl" >/dev/null
grep -F 'runner=["' "$tmp/measure/chrome-window-snapshot.log" >/dev/null
grep -Fx 'firefox=' "$tmp/measure/chrome-window-snapshot.log" >/dev/null
grep -Fx 'chrome=' "$tmp/measure/firefox-worker-snapshot.log" >/dev/null
grep -Fx '<tests/browser/.cargo/config.toml>' "$tmp/measure/firefox-worker-snapshot.log" >/dev/null
"${recipe[@]}" browser-native "$tmp/native" 5 > "$tmp/run.log" 2>&1
jq -se 'length == 8 and all(.status == "passed")' "$tmp/native/results.jsonl" >/dev/null
# Stub nested CRAP through Cargo as well.
"${recipe[@]}" browser-final "$tmp/final" 5 > "$tmp/run.log" 2>&1
jq -se 'length == 8 and all(.status == "passed")' "$tmp/final/results.jsonl" >/dev/null
echo 'PASS: measurements/config discovery, native regressions and final gates'
if MODE=gates "${recipe[@]}" browser-final "$tmp/final-failed" 5 > "$tmp/run.log" 2>&1; then exit 1; fi
jq -se 'length == 8 and any(.[]; .name == "clippy" and .status == "failed" and .exit_code == 9) and any(.[]; .name == "feature-powerset" and .status == "failed") and any(.[]; .name == "crap" and .status == "passed")' "$tmp/final-failed/results.jsonl" >/dev/null
CHROMEDRIVER='environment driver' "${recipe[@]}" browser-accept chrome worker "$tmp/focused" 5 > "$tmp/run.log" 2>&1
jq -se 'length == 5 and .[-1].name == "chrome-worker"' "$tmp/focused/results.jsonl" >/dev/null
grep -Fx '<environment driver>' "$tmp/focused/chrome-worker.log" >/dev/null
if "${recipe[@]}" browser-measure chrome worker "$tmp/no-runner" 5 "$tmp/missing runner" 'chrome space' > "$tmp/run.log" 2>&1; then exit 1; fi
jq -se 'any(.[]; .name == "measurement-runner" and .status == "unavailable")' "$tmp/no-runner/results.jsonl" >/dev/null
echo 'PASS: later final gates survive failures, focused cell/environment defaults, missing measurement runner'
