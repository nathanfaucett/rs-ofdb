#!/usr/bin/env python3
"""Bounded browser release checks; run from any directory. Python standard library only."""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = "wasm32-unknown-unknown"
BROWSER_FEATURES = (
    ("ofdb-kv", "indexeddb"),
    ("ofdb-kv", "indexeddb,sync"),
    ("ofdb-sql", "indexeddb"),
    ("ofdb-sql", "indexeddb,sql"),
    ("ofdb-sql", "indexeddb,sync"),
    ("ofdb-sql", "indexeddb,sql,sync"),
    ("ofdb-sql", "indexeddb,sql,sync,native-stage"),
)


def forbidden_packages(tree, remote=False):
    packages = {line.split()[0] for line in tree.splitlines() if line.strip()}
    if remote:
        return sorted(
            p
            for p in packages
            if p in {"redb", "automerge"}
            or p.startswith(
                (
                    "ofdb-btree",
                    "ofdb-sql-engine",
                    "ofdb-kv-store",
                    "ofdb-sql-sync",
                    "ofdb-kv-sync",
                    "ofdb-sync-runtime",
                )
            )
        )
    return sorted(
        p
        for p in packages
        if p
        in {
            "redb",
            "tempfile",
            "mio",
            "socket2",
            "fs2",
            "fd-lock",
            "rustix",
            "libc",
            "tokio-stream",
            "hyper",
            "hyper-util",
            "tower",
            "tonic",
            "tonic-prost",
        }
        or p.startswith(
            (
                "iroh",
                "ofdb-btree-redb",
                "ofdb-sql-engine-redb",
                "ofdb-resource",
                "ofdb-sql-client",
                "ofdb-kv-client",
                "ofdb-sql-server",
                "ofdb-kv-server",
                "ofdb-sql-proto",
                "ofdb-kv-proto",
            )
        )
    )


def browser_command(browser, context, driver):
    command = ["wasm-pack", "test", "--headless", f"--{browser}"]
    if driver:
        command += [
            f"--{'chromedriver' if browser == 'chrome' else 'geckodriver'}",
            driver,
        ]
    return command + [
        "tests/browser",
        "--features",
        "worker,sync" if context == "worker" else "sync",
        "--lib",
    ]


class Checks:
    def __init__(self, output, timeout):
        self.output = output
        self.timeout = timeout
        self.results = []
        output.mkdir(parents=True, exist_ok=True)

    def run(self, name, command, graph=None, extra_env=None):
        log = self.output / f"{name}.log"
        print(f"\n[{name}] {' '.join(command)} -> {log}", flush=True)
        started = time.monotonic()
        env = dict(os.environ)
        if extra_env:
            for key, value in extra_env.items():
                if value is None:
                    env.pop(key, None)
                else:
                    env[key] = value
        env.setdefault("CARGO_BUILD_JOBS", "2")
        env.setdefault("RUST_MIN_STACK", "16777216")
        env.setdefault("WASM_BINDGEN_TEST_TIMEOUT", "240")
        status, code = "failed", None
        with log.open("w") as stream:
            stream.write(f"cwd: {ROOT}\ncommand: {command!r}\n")
            stream.flush()
            try:
                process = subprocess.Popen(
                    command,
                    cwd=ROOT,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    code = process.wait(timeout=self.timeout)
                    status = "passed" if code == 0 else "failed"
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    status = "timeout"
            except OSError as error:
                stream.write(f"{error}\n")
                status = "unavailable"
        if status == "passed" and graph:
            text = log.read_text()
            # Ignore the two runner metadata lines, never Cargo's exit status.
            rejected = forbidden_packages(
                "\n".join(text.splitlines()[2:]), remote=graph == "remote"
            )
            if rejected:
                with log.open("a") as stream:
                    stream.write(f"\nForbidden normal dependencies: {rejected}\n")
                status = "failed"
        result = dict(
            name=name,
            command=command,
            status=status,
            exit_code=code,
            seconds=round(time.monotonic() - started, 2),
            log=str(log),
        )
        self.results.append(result)
        (self.output / "results.json").write_text(
            json.dumps(self.results, indent=2) + "\n"
        )
        print(f"{status}: {name} ({result['seconds']}s)", flush=True)
        if status != "passed":
            print("\n".join(log.read_text().splitlines()[-35:]), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "suite", choices=("check", "browser", "measure", "native", "final")
    )
    parser.add_argument(
        "--browser", choices=("chrome", "firefox", "all"), default="all"
    )
    parser.add_argument("--context", choices=("window", "worker", "all"), default="all")
    parser.add_argument("--chromedriver", default=os.environ.get("CHROMEDRIVER"))
    parser.add_argument("--geckodriver", default=os.environ.get("GECKODRIVER"))
    parser.add_argument(
        "--runner",
        default=shutil.which("wasm-bindgen-test-runner"),
        help="matching wasm-bindgen-test-runner, required for measure",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="seconds per command, including compilation",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "target/browser-matrix")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.suite == "measure" and not args.runner:
        parser.error(
            "measure requires --runner PATH or wasm-bindgen-test-runner on PATH"
        )
    checks = Checks(args.output.resolve(), args.timeout)
    run = checks.run
    run("rustc", ["rustc", "--version", "--verbose"])
    run("cargo", ["cargo", "--version"])
    run("revision", ["git", "--no-pager", "rev-parse", "HEAD"])
    if args.suite == "check":
        for package, features in BROWSER_FEATURES:
            name = f"{package}-{features.replace(',', '-')}"
            common = ["-p", package, "--no-default-features", "--features", features]
            run(
                name,
                ["cargo", "check", "--locked", *common, "--lib", "--target", TARGET],
            )
            run(
                name + "-graph",
                [
                    "cargo",
                    "tree",
                    "--locked",
                    *common,
                    "--target",
                    TARGET,
                    "--edges",
                    "normal",
                    "--prefix",
                    "none",
                    "--format",
                    "{p}",
                ],
                "browser",
            )
        for package in ("ofdb-sql", "ofdb-kv"):
            run(
                package + "-remote-graph",
                [
                    "cargo",
                    "tree",
                    "--locked",
                    "-p",
                    package,
                    "--no-default-features",
                    "--features",
                    "remote",
                    "--edges",
                    "normal",
                    "--prefix",
                    "none",
                    "--format",
                    "{p}",
                ],
                "remote",
            )
        run(
            "fixture-graph",
            [
                "cargo",
                "tree",
                "--locked",
                "--manifest-path",
                "tests/browser/Cargo.toml",
                "--features",
                "sync,ofdb-sql/native-stage",
                "--target",
                TARGET,
                "--edges",
                "normal",
                "--prefix",
                "none",
                "--format",
                "{p}",
            ],
            "browser",
        )
        for context, features in (("window", "sync"), ("worker", "worker,sync")):
            run(
                context + "-clippy",
                [
                    "cargo",
                    "clippy",
                    "--locked",
                    "--manifest-path",
                    "tests/browser/Cargo.toml",
                    "--tests",
                    "--target",
                    TARGET,
                    "--features",
                    features,
                    "--",
                    "-D",
                    "warnings",
                ],
            )
        quickstart = ROOT / "target/browser-quickstarts"
        (quickstart / "src").mkdir(parents=True, exist_ok=True)
        (quickstart / "Cargo.toml").write_text("""[package]
name = "browser-quickstarts"
version = "0.0.0"
edition = "2024"
[workspace]
[dependencies]
ofdb-kv = { path = "../../crates/kv", default-features = false, features = ["indexeddb", "sync"] }
ofdb-sql = { path = "../../crates/sql", default-features = false, features = ["indexeddb", "sql", "sync"] }
""")
        shutil.copyfile(ROOT / "tests/browser/Cargo.lock", quickstart / "Cargo.lock")
        blocks = re.findall(
            r"```rust\n(.*?)```", (ROOT / "docs/browser.md").read_text(), re.S
        )
        (quickstart / "src/lib.rs").write_text("pub mod quickstarts;\n")
        (quickstart / "src/quickstarts.rs").write_text("\n".join(blocks))
        run(
            "public-quickstarts",
            [
                "cargo",
                "check",
                "--manifest-path",
                str(quickstart / "Cargo.toml"),
                "--target",
                TARGET,
            ],
        )
    elif args.suite == "measure":
        for browser in (
            ("chrome", "firefox") if args.browser == "all" else (args.browser,)
        ):
            driver_name = "CHROMEDRIVER" if browser == "chrome" else "GECKODRIVER"
            driver = (
                args.chromedriver if browser == "chrome" else args.geckodriver
            ) or shutil.which(driver_name.lower())
            if not driver:
                parser.error(
                    f"measure requires --{driver_name.lower()} PATH or driver on PATH"
                )
            for context in (
                ("window", "worker") if args.context == "all" else (args.context,)
            ):
                env = {
                    "CHROMEDRIVER": None,
                    "GECKODRIVER": None,
                    "CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER": str(
                        Path(args.runner).resolve()
                    ),
                    "CARGO_TARGET_DIR": str(ROOT / "tests/browser/target"),
                    driver_name: driver,
                }
                run(
                    f"{browser}-{context}-snapshot",
                    [
                        "cargo",
                        "test",
                        "--locked",
                        "--config",
                        "tests/browser/.cargo/config.toml",
                        "--manifest-path",
                        "tests/browser/Cargo.toml",
                        "--lib",
                        "--target",
                        TARGET,
                        "--features",
                        "worker,sync" if context == "worker" else "sync",
                        "snapshot_materialization_measurement",
                        "--",
                        "--nocapture",
                    ],
                    extra_env=env,
                )
    elif args.suite == "browser":
        run("wasm-pack", ["wasm-pack", "--version"])
        for browser in (
            ("chrome", "firefox") if args.browser == "all" else (args.browser,)
        ):
            for context in (
                ("window", "worker") if args.context == "all" else (args.context,)
            ):
                driver = args.chromedriver if browser == "chrome" else args.geckodriver
                run(f"{browser}-{context}", browser_command(browser, context, driver))
    elif args.suite == "native":
        run(
            "native-protocol-ownership",
            [
                "cargo",
                "test",
                "--locked",
                "--manifest-path",
                "tests/browser/Cargo.toml",
                "--features",
                "sync",
                "--test",
                "native",
                "--test",
                "sql_protocol",
                "--test",
                "kv_protocol",
            ],
        )
        run(
            "native-kv",
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "ofdb-kv",
                "--features",
                "in-memory,redb,sync",
                "--test",
                "embedded_client",
                "--test",
                "embedded_redb_client",
                "--test",
                "changes",
                "--test",
                "facade_sync",
            ],
        )
        run(
            "native-sql",
            [
                "cargo",
                "test",
                "--locked",
                "-p",
                "ofdb-sql",
                "--features",
                "in-memory,redb,sql,sync,native-stage",
                "--test",
                "local_sql",
                "--test",
                "changes",
                "--test",
                "commit_invalidation",
                "--test",
                "scoped_sync",
                "--test",
                "facade_sync",
                "--test",
                "conflicts",
            ],
        )
        run(
            "native-runtime",
            ["cargo", "test", "--locked", "-p", "ofdb-sync-runtime", "--lib"],
        )
        run(
            "native-clippy",
            [
                "cargo",
                "clippy",
                "--locked",
                "-p",
                "ofdb-sql",
                "-p",
                "ofdb-kv",
                "-p",
                "ofdb-sync-runtime",
                "--features",
                "in-memory,redb,indexeddb,sql,sync,native-stage",
                "--",
                "-D",
                "warnings",
            ],
        )
    else:
        run("fmt", ["cargo", "fmt", "--all", "--", "--check"])
        run(
            "fixture-fmt",
            [
                "cargo",
                "fmt",
                "--manifest-path",
                "tests/browser/Cargo.toml",
                "--all",
                "--",
                "--check",
            ],
        )
        run(
            "feature-powerset",
            ["cargo", "hack", "test", "--feature-powerset", "--all-targets"],
        )
        run("crap", ["just", "crap"])
    return int(any(result["status"] != "passed" for result in checks.results))


if __name__ == "__main__":
    sys.exit(main())
