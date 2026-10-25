import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import browser_matrix as matrix


class MatrixTests(unittest.TestCase):
    def test_supported_graph_rejects_native_io(self):
        self.assertEqual(
            matrix.forbidden_packages("ofdb-sql v0.1.0\ntokio v1.52.0\n"), []
        )
        for package in (
            "redb",
            "ofdb-btree-redb",
            "ofdb-sql-engine-redb",
            "tempfile",
            "tonic",
            "iroh",
            "ofdb-kv-proto",
            "ofdb-sql-server",
            "mio",
            "socket2",
            "fs2",
        ):
            with self.subTest(package=package):
                self.assertEqual(
                    matrix.forbidden_packages(f"{package} v1.0.0\n"), [package]
                )

    def test_remote_isolation_is_store_specific(self):
        self.assertEqual(
            matrix.forbidden_packages("ofdb-kv-store v0.1.0\n", remote=True),
            ["ofdb-kv-store"],
        )
        self.assertEqual(
            matrix.forbidden_packages("automerge v0.10.0\n", remote=True), ["automerge"]
        )

    def test_browser_commands_run_full_library_in_real_contexts(self):
        window = matrix.browser_command("chrome", "window", None)
        worker = matrix.browser_command("firefox", "worker", "/tools/geckodriver")
        self.assertIn("--chrome", window)
        self.assertEqual(window[-3:], ["--features", "sync", "--lib"])
        self.assertIn("--geckodriver", worker)
        self.assertEqual(worker[-3:], ["--features", "worker,sync", "--lib"])

    def test_results_preserve_failure_unavailable_and_timeout(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            checks = matrix.Checks(Path(temporary), 0.2)
            checks.run("failure", [sys.executable, "-c", "raise SystemExit(7)"])
            checks.run("missing", [str(Path(temporary) / "absent-tool")])
            checks.run("timeout", [sys.executable, "-c", "import time; time.sleep(10)"])
            results = json.loads((Path(temporary) / "results.json").read_text())
            self.assertEqual(
                [r["status"] for r in results], ["failed", "unavailable", "timeout"]
            )
            self.assertEqual(results[0]["exit_code"], 7)

    def test_successful_cargo_exit_cannot_hide_forbidden_dependency(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            checks = matrix.Checks(Path(temporary), 5)
            checks.run(
                "graph", [sys.executable, "-c", "print('redb v4.3.0')"], graph="browser"
            )
            self.assertEqual(checks.results[0]["status"], "failed")
            self.assertEqual(checks.results[0]["exit_code"], 0)

    def test_each_facade_has_independent_minimal_feature_checks(self):
        self.assertIn(("ofdb-kv", "indexeddb"), matrix.BROWSER_FEATURES)
        self.assertIn(("ofdb-kv", "indexeddb,sync"), matrix.BROWSER_FEATURES)
        self.assertIn(("ofdb-sql", "indexeddb"), matrix.BROWSER_FEATURES)
        self.assertIn(("ofdb-sql", "indexeddb,sql"), matrix.BROWSER_FEATURES)
        self.assertIn(("ofdb-sql", "indexeddb,sync"), matrix.BROWSER_FEATURES)
        self.assertIn(
            ("ofdb-sql", "indexeddb,sql,sync,native-stage"), matrix.BROWSER_FEATURES
        )


if __name__ == "__main__":
    unittest.main()
