#!/usr/bin/env bash
set -euo pipefail
quickstart=target/browser-quickstarts
mkdir -p "$quickstart/src"
cat > "$quickstart/Cargo.toml" <<'TOML'
[package]
name = "browser-quickstarts"
version = "0.0.0"
edition = "2024"
[workspace]
[dependencies]
ofdb-kv = { path = "../../crates/kv", default-features = false, features = ["indexeddb", "sync"] }
ofdb-sql = { path = "../../crates/sql", default-features = false, features = ["indexeddb", "sql", "sync"] }
TOML
cp tests/browser/Cargo.lock "$quickstart/Cargo.lock"
printf 'pub mod quickstarts;\n' > "$quickstart/src/lib.rs"
awk '/^```rust$/ { rust=1; count++; next } /^```$/ { rust=0 } rust { print } END { if (count != 4) exit 1 }' docs/browser.md > "$quickstart/src/quickstarts.rs"
cargo check --manifest-path "$quickstart/Cargo.toml" --target wasm32-unknown-unknown
