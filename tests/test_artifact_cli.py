import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from forgetrain.artifacts import ArtifactRegistry


class ArtifactCLITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.outputs = self.root / "outputs"
        self.outputs.mkdir()
        self.source = self.outputs / "checkpoint.bin"
        self.source.write_bytes(b"synthetic artifact not a model")
        self.database = self.root / "artifacts.sqlite"
        self.metadata = self.root / "metadata.json"
        self.row = {
            "schema_version": 1,
            "id": "checkpoint-1",
            "relative_path": "checkpoint.bin",
            "kind": "checkpoint",
            "format": "synthetic-raw",
            "producer_revision": "a" * 40,
        }
        self.write()

    def write(self, **changes):
        self.metadata.write_text(json.dumps(dict(self.row, **changes)))

    def command(self, *args, code=0):
        result = subprocess.run(
            [sys.executable, "-m", "forgetrain.artifact_cli", *map(str, args)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, code, result.stderr)
        if code == 2:
            self.assertEqual(result.stdout, "")
            self.assertTrue(result.stderr)
            return None
        self.assertEqual(result.stderr, "")
        report = json.loads(result.stdout)
        self.assertEqual(report["schema_version"], 1)
        return report

    def create(self):
        self.command("create", self.database, self.outputs)

    def register(self, **changes):
        self.write(**changes)
        return self.command("register", self.database, self.metadata)["registration"]

    def test_create_register_restart_idempotence_and_verify(self):
        self.create()
        first = self.register()
        self.assertEqual(first["size_bytes"], len(self.source.read_bytes()))
        self.assertEqual(self.register(), first)
        report = self.command("verify", self.database, "checkpoint-1")["verification"]
        self.assertTrue(report["matches"])
        self.assertFalse(report["checkpoint_validity_verified"])
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM registrations").fetchone()[0], 1
            )

    def test_conflict_and_content_drift_preserve_original_record(self):
        self.create()
        original = self.register()
        self.write(format="conflicting")
        self.command("register", self.database, self.metadata, code=2)
        self.source.write_bytes(b"drift")
        self.write()
        self.command("register", self.database, self.metadata, code=2)
        self.assertEqual(ArtifactRegistry(self.database).get("checkpoint-1"), original)
        report = self.command("verify", self.database, "checkpoint-1", code=1)
        self.assertFalse(report["verification"]["matches"])
        self.source.unlink()
        self.command("verify", self.database, "checkpoint-1", code=2)

    def test_unsafe_paths_and_nonregular_artifacts_fail_without_registration(self):
        self.create()
        outside = self.root / "outside"
        outside.write_text("synthetic outside")
        (self.outputs / "link").symlink_to(outside)
        for path in ["../outside", str(outside), "./checkpoint.bin", "link", "missing"]:
            self.write(relative_path=path)
            self.command("register", self.database, self.metadata, code=2)
        if hasattr(os, "mkfifo"):
            os.mkfifo(self.outputs / "pipe")
            self.write(relative_path="pipe")
            self.command("register", self.database, self.metadata, code=2)
        self.assertEqual(self.command("list", self.database)["page"]["records"], [])
        self.assertEqual(outside.read_text(), "synthetic outside")

    def test_strict_metadata_and_size_limits_fail_before_mutation(self):
        self.create()
        for changes in [
            {"schema_version": True},
            {"schema_version": 2},
            {"extra": 1},
            {"id": "../bad"},
            {"producer_revision": "main"},
            {"format": ""},
            {"kind": []},
        ]:
            self.write(**changes)
            self.command("register", self.database, self.metadata, code=2)
        for raw in [
            b"{}",
            b"\xff",
            b'{"id":1,"id":2}',
            b'{"x":NaN}',
            b" " * (16 * 1024 + 1),
            b"[" * 2000 + b"]" * 2000,
        ]:
            self.metadata.write_bytes(raw)
            self.command("register", self.database, self.metadata, code=2)
        if hasattr(os, "mkfifo"):
            pipe = self.root / "metadata-pipe"
            os.mkfifo(pipe)
            self.command("register", self.database, pipe, code=2)
        self.assertEqual(self.command("list", self.database)["page"]["records"], [])

    def test_exclusive_create_missing_and_corrupt_database(self):
        self.create()
        original = self.database.read_bytes()
        self.command("create", self.database, self.outputs, code=2)
        self.assertEqual(self.database.read_bytes(), original)
        missing = self.root / "missing.sqlite"
        self.command("list", missing, code=2)
        self.assertFalse(missing.exists())
        invalid_root = self.root / "nonexistent"
        self.command("create", missing, invalid_root, code=2)
        self.assertFalse(missing.exists())
        corrupt = self.root / "corrupt.sqlite"
        corrupt.write_bytes(b"not a registry")
        self.command("list", corrupt, code=2)
        self.assertEqual(corrupt.read_bytes(), b"not a registry")

    def test_bounded_keyset_pages_are_sorted_exclusive_and_nonmutating(self):
        self.create()
        for identifier in ["z", "a", "m"]:
            self.register(id=identifier)
        first = self.command("list", self.database, "--limit", 2)["page"]
        self.assertEqual([r["id"] for r in first["records"]], ["a", "m"])
        self.assertTrue(first["has_more"])
        self.assertFalse(first["content_files_verified"])
        self.assertEqual(first["next_after"], "m")
        second = self.command(
            "list", self.database, "--limit", 2, "--after", first["next_after"]
        )["page"]
        self.assertEqual([r["id"] for r in second["records"]], ["z"])
        self.assertFalse(second["has_more"])
        self.assertIsNone(second["next_after"])
        self.assertEqual(
            self.command("list", self.database, "--after", "zz")["page"]["records"], []
        )
        for args in [("--limit", 0), ("--limit", 101), ("--after", "../escape")]:
            self.command("list", self.database, *args, code=2)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM registrations").fetchone()[0], 3
            )

    def test_library_page_limits_and_cursor_types(self):
        self.create()
        registry = ArtifactRegistry(self.database)
        for limit in [True, 0, 101, 1.5]:
            with self.assertRaises(ValueError):
                registry.list_page(limit=limit)
        for after in ["", True, "../bad"]:
            with self.assertRaises(ValueError):
                registry.list_page(after=after)


if __name__ == "__main__":
    unittest.main()
