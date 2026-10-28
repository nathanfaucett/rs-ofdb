# Sourced by browser recipes; command selection stays in the justfile.
set -euo pipefail

begin() {
    output=$1 budget=$2 failed=0 active='' current=''
    [[ $budget =~ ^[0-9]+$ && $budget =~ [1-9] ]] || { echo 'timeout must be positive integer seconds' >&2; exit 2; }
    for tool in timeout setsid jq awk; do
        command -v "$tool" >/dev/null || { echo "required utility unavailable: $tool" >&2; exit 2; }
    done
    mkdir -p -- "$output"
    output=$(realpath -- "$output")
    : > "$output/results.jsonl"
    export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
    export RUST_MIN_STACK=${RUST_MIN_STACK:-16777216}
    export WASM_BINDGEN_TEST_TIMEOUT=${WASM_BINDGEN_TEST_TIMEOUT:-240}
    jq -cn 'env | {CARGO_BUILD_JOBS,RUST_MIN_STACK,WASM_BINDGEN_TEST_TIMEOUT,WASM_BINDGEN_TEST_WEBDRIVER_JSON,CARGO_TARGET_DIR,RUSTFLAGS}' > "$output/environment.json"
    trap 'interrupt 130' INT
    trap 'interrupt 143' TERM
    trap 'cleanup' EXIT
    check rustc rustc --version --verbose
    check cargo cargo --version
    check revision git --no-pager rev-parse HEAD
}

cleanup() {
    if [[ -n $active ]]; then
        kill -TERM -- "-$active" 2>/dev/null || true
        sleep 1
        kill -KILL -- "-$active" 2>/dev/null || true
        wait "$active" 2>/dev/null || true
        active=''
    fi
}

interrupt() {
    trap '' INT TERM
    cleanup
    if [[ -n $current ]]; then record interrupted "$1"; fi
    exit "$1"
}

record() {
    local status=$1 code=$2
    jq -cn --arg name "$current" --arg status "$status" --argjson exit_code "$code" \
        --argjson seconds "$((SECONDS - started))" --arg log "$log" \
        --arg cwd "$PWD" --arg started_at "$started_at" --args \
        '{name:$name,status:$status,exit_code:$exit_code,seconds:$seconds,log:$log,cwd:$cwd,started_at:$started_at,command:$ARGS.positional}' \
        -- "${command[@]}" >> "$output/results.jsonl"
    printf '%s: %s (%ss) -> %s\n' "$status" "$current" "$((SECONDS - started))" "$log"
    [[ $status == passed ]] || failed=1
    current=''
}

check() {
    local policy=${GRAPH_POLICY:-} code status
    current=$1; shift
    command=("$@")
    log="$output/$current.log" started=$SECONDS
    started_at=$(date -u +%FT%TZ)
    printf '\n[%s] ' "$current"; printf '%q ' "$@"; printf '\n'
    # A fresh group lets timeout and interruption reach inherited descendants.
    setsid timeout --signal=TERM --kill-after=5 "$budget" "$@" > "$log" 2>&1 &
    active=$!
    code=0
    wait "$active" || code=$?
    # timeout may exit when its direct child does, before stubborn descendants.
    kill -KILL -- "-$active" 2>/dev/null || true
    active=''
    status=failed
    case $code in
        0) status=passed ;;
        124|137) status=timeout ;;
        126|127) status=unavailable ;;
    esac
    if [[ $status == passed && -n $policy ]]; then
        if ! awk -v policy="$policy" '
            policy == "remote" && ($1 ~ /^(redb|automerge)$/ || $1 ~ /^(ofdb-btree|ofdb-sql-engine|ofdb-kv-store|ofdb-sql-sync|ofdb-kv-sync|ofdb-sync-runtime)/) { bad=1; print "Forbidden normal dependency: " $1 > "/dev/stderr" }
            policy == "browser" && ($1 ~ /^(redb|tempfile|mio|socket2|fs2|fd-lock|rustix|libc|tokio-stream|hyper|hyper-util|tower|tonic|tonic-prost)$/ || $1 ~ /^(iroh|ofdb-btree-redb|ofdb-sql-engine-redb|ofdb-resource|ofdb-sql-client|ofdb-kv-client|ofdb-sql-server|ofdb-kv-server|ofdb-sql-proto|ofdb-kv-proto)/) { bad=1; print "Forbidden normal dependency: " $1 > "/dev/stderr" }
            END { exit bad }
        ' "$log" 2> "$output/$current.policy.log"; then status=failed; fi
    fi
    record "$status" "$code"
}

selection() {
    case $1 in chrome|firefox) browsers=("$1");; all) browsers=(chrome firefox);; *) echo 'browser must be chrome, firefox or all' >&2; exit 2;; esac
    case $2 in window|worker) contexts=("$2");; all) contexts=(window worker);; *) echo 'context must be window, worker or all' >&2; exit 2;; esac
}

finish() { exit "$failed"; }
