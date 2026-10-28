set shell := ["bash", "-cu"]

# Default task when invoking `just` with no arguments.
default: help

help:
    @printf "Available recipes:\n"
    @printf "  build          Build all workspace crates\n"
    @printf "  build-release  Build all workspace crates in release mode\n"
    @printf "  check          Check all workspace crates\n"
    @printf "  test           Run workspace tests\n"
    @printf "  clippy         Run clippy for all targets and workspace crates\n"
    @printf "  clippy-fix     Run clippy with --fix for all targets and workspace crates\n"
    @printf "  crap           Run CRAP\n"
    @printf "  crap-summary   Run CRAP and get a summary\n"
    @printf "  fmt            Format all workspace crates\n"
    @printf "  fmt-check      Check formatting for all workspace crates\n"
    @printf "  clean          Remove build artifacts\n"
    @printf "  doc            Build workspace documentation\n"
    @printf "  browser-{check,accept,measure,native,final}  Bounded release validation (just --list)\n"
    @printf "  browser-orchestration-test  Controlled public-recipe checks\n"
    @printf "  browser-example-build  Build the persistent KV and SQL Wasm page\n"
    @printf "  browser-example-serve  Build and serve at http://127.0.0.1:8080\n"
    @printf "  browser-example-smoke  Build and check the page in Chromium and Firefox\n"

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

check:
    cargo check --workspace

test:
    cargo hack test --feature-powerset --workspace --all-targets

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

clippy-fix:
    cargo clippy --workspace --all-targets --fix --allow-dirty --broken-code -- -D warnings

crap *args:
    RUST_MIN_STACK=67108864 cargo hack llvm-cov --workspace --lcov --output-path /tmp/lcov.info && cargo crap --workspace --lcov /tmp/lcov.info  {{ args }}

crap-summary:
    just crap --summary

cov:
    RUST_MIN_STACK=67108864 cargo llvm-cov --workspace --open

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clean:
    cargo clean

doc:
    cargo doc --workspace --no-deps

browser-example-build:
    (cd examples/browser && wasm-pack build . --target web --dev --no-pack -- --locked)

browser-example-serve port="8080": browser-example-build
    cargo run --locked --manifest-path examples/browser/tools/Cargo.toml -- serve {{ port }}

browser-example-smoke: browser-example-build
    cargo run --locked --manifest-path examples/browser/tools/Cargo.toml -- smoke

# Independent facade builds, dependency policies, browser Clippy and published quickstarts.
[positional-arguments]
browser-check output="target/browser-validation/check" timeout="600":
    #!/usr/bin/env bash
    source scripts/browser-evidence.sh
    begin "$1" "$2"
    target=wasm32-unknown-unknown
    for package in ofdb-kv ofdb-sql; do
        features=(indexeddb indexeddb,sync)
        if [[ $package == ofdb-sql ]]; then
            features=(indexeddb indexeddb,sql indexeddb,sync indexeddb,sql,sync indexeddb,sql,sync,native-stage)
        fi
        for feature in "${features[@]}"; do
            name="$package-${feature//,/-}"
            common=(-p "$package" --no-default-features --features "$feature")
            check "$name" cargo check --locked "${common[@]}" --lib --target "$target"
            GRAPH_POLICY=browser check "$name-graph" cargo tree --locked "${common[@]}" --target "$target" --edges normal --prefix none --format '{p}'
        done
        GRAPH_POLICY=remote check "$package-remote-graph" cargo tree --locked -p "$package" --no-default-features --features remote --edges normal --prefix none --format '{p}'
    done
    GRAPH_POLICY=browser check fixture-graph cargo tree --locked --manifest-path tests/browser/Cargo.toml --features sync,ofdb-sql/native-stage --target "$target" --edges normal --prefix none --format '{p}'
    for context in window worker; do
        feature=sync
        [[ $context != worker ]] || feature=worker,sync
        check "$context-clippy" cargo clippy --locked --manifest-path tests/browser/Cargo.toml --tests --target "$target" --features "$feature" -- -D warnings
    done
    check public-quickstarts bash scripts/browser-quickstarts.sh
    finish

# Full Rust library acceptance; select chrome|firefox|all and window|worker|all.
[positional-arguments]
browser-accept browser="all" context="all" output="target/browser-validation/browser" timeout="600" chromedriver=env_var_or_default("CHROMEDRIVER", "") geckodriver=env_var_or_default("GECKODRIVER", ""):
    #!/usr/bin/env bash
    source scripts/browser-evidence.sh
    selection "$1" "$2"
    begin "$3" "$4"
    check wasm-pack wasm-pack --version
    for browser in "${browsers[@]}"; do
        driver=$5; option=--chromedriver
        if [[ $browser == firefox ]]; then driver=$6; option=--geckodriver; fi
        driver_args=()
        [[ -z $driver ]] || driver_args=("$option" "$driver")
        for context in "${contexts[@]}"; do
            feature=sync
            [[ $context != worker ]] || feature=worker,sync
            check "$browser-$context" wasm-pack test --headless "--$browser" "${driver_args[@]}" tests/browser --features "$feature" --lib
        done
    done
    finish

# Filtered snapshot diagnostics; runner must match the fixture wasm-bindgen lock.
[positional-arguments]
browser-measure browser="all" context="all" output="target/browser-validation/measure" timeout="600" runner="wasm-bindgen-test-runner" chromedriver=env_var_or_default("CHROMEDRIVER", "") geckodriver=env_var_or_default("GECKODRIVER", ""):
    #!/usr/bin/env bash
    source scripts/browser-evidence.sh
    selection "$1" "$2"
    begin "$3" "$4"
    runner=$(command -v -- "$5" || printf '%s' "$5")
    [[ ! -f $runner ]] || runner=$(realpath -- "$runner")
    check measurement-runner "$runner" --version
    runner_config=$(jq -cn --arg runner "$runner" '[$runner]')
    for browser in "${browsers[@]}"; do
        driver=$6; key=CHROMEDRIVER; tool=chromedriver
        if [[ $browser == firefox ]]; then driver=$7; key=GECKODRIVER; tool=geckodriver; fi
        if [[ -z $driver ]]; then driver=$(command -v "$tool" || printf '%s' "$tool"); fi
        check "$browser-driver" "$driver" --version
        for context in "${contexts[@]}"; do
            feature=sync
            [[ $context != worker ]] || feature=worker,sync
            check "$browser-$context-snapshot" env -u CHROMEDRIVER -u GECKODRIVER -u CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER "$key=$driver" "CARGO_TARGET_DIR=$PWD/tests/browser/target" cargo test --locked --config tests/browser/.cargo/config.toml --config "target.wasm32-unknown-unknown.runner=$runner_config" --manifest-path tests/browser/Cargo.toml --lib --target wasm32-unknown-unknown --features "$feature" snapshot_materialization_measurement -- --nocapture
        done
    done
    finish

# Native protocol, ownership, storage and sync regressions plus focused Clippy.
[positional-arguments]
browser-native output="target/browser-validation/native" timeout="600":
    #!/usr/bin/env bash
    source scripts/browser-evidence.sh
    begin "$1" "$2"
    check native-protocol-ownership cargo test --locked --manifest-path tests/browser/Cargo.toml --features sync --test native --test sql_protocol --test kv_protocol
    check native-kv cargo test --locked -p ofdb-kv --features in-memory,redb,sync --test embedded_client --test embedded_redb_client --test changes --test facade_sync
    check native-sql cargo test --locked -p ofdb-sql --features in-memory,redb,sql,sync,native-stage --test local_sql --test changes --test commit_invalidation --test scoped_sync --test facade_sync --test conflicts
    check native-runtime cargo test --locked -p ofdb-sync-runtime --lib
    check native-clippy cargo clippy --locked -p ofdb-sql -p ofdb-kv -p ofdb-sync-runtime --features in-memory,redb,indexeddb,sql,sync,native-stage -- -D warnings
    finish

# Independent final gates; failures never skip later evidence.
[positional-arguments]
browser-final output="target/browser-validation/final" timeout="240":
    #!/usr/bin/env bash
    source scripts/browser-evidence.sh
    begin "$1" "$2"
    check fmt cargo fmt --all -- --check
    check fixture-fmt cargo fmt --manifest-path tests/browser/Cargo.toml --all -- --check
    check clippy cargo clippy --workspace --all-targets -- -D warnings
    check feature-powerset cargo hack test --feature-powerset --all-targets
    check crap just crap
    finish

# Controlled public-recipe checks; no browsers or compilation required.
browser-orchestration-test:
    timeout --kill-after=5 180 bash scripts/test-browser-validation.sh
