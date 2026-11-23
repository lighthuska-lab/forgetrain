import concurrent.futures
import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from forgetrain.artifacts import ArtifactRegistry


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.root = self.directory / "outputs"
        self.root.mkdir()
        self.source = self.root / "checkpoint.bin"
        self.source.write_bytes(b"synthetic-checkpoint-bytes\n")
        self.path = self.directory / "registry.sqlite"
        self.registry = ArtifactRegistry.create(self.path, root=self.root)

    def register(self, **changes):
        args = {
            "relative_path": "checkpoint.bin",
            "kind": "checkpoint",
            "format": "synthetic-not-a-model",
            "producer_revision": "a" * 40,
        }
        args.update(changes)
        return self.registry.register("checkpoint-1", **args)

    def test_actual_bytes_metadata_restart_and_detached_return(self):
        record = self.register()
        self.assertEqual(
            record["sha256"], hashlib.sha256(self.source.read_bytes()).hexdigest()
        )
        self.assertEqual(record["size_bytes"], len(self.source.read_bytes()))
        self.assertIn("+00:00", record["registered_at"])
        self.assertEqual(ArtifactRegistry(self.path).get("checkpoint-1"), record)
        self.assertTrue(ArtifactRegistry(self.path).verify("checkpoint-1")["matches"])
        self.assertFalse(
            self.registry.verify("checkpoint-1")["checkpoint_validity_verified"]
        )
        record["sha256"] = "0" * 64
        self.assertNotEqual(record, self.registry.get("checkpoint-1"))

    def test_identical_registration_is_idempotent_with_one_event(self):
        first = self.register()
        self.assertEqual(self.register(), first)
        with closing(sqlite3.connect(self.path)) as db, db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM registrations").fetchone()[0], 1
            )

    def test_conflicting_metadata_or_content_cannot_replace_id(self):
        first = self.register()
        for changes in [
            {"kind": "metrics"},
            {"format": "another-format"},
            {"producer_revision": "b" * 40},
        ]:
            with (
                self.subTest(changes=changes),
                self.assertRaisesRegex(ValueError, "conflicting"),
            ):
                self.register(**changes)
            self.assertEqual(self.registry.get("checkpoint-1"), first)
        self.source.write_bytes(b"mutated")
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.register()
        self.assertEqual(self.registry.get("checkpoint-1"), first)
        report = self.registry.verify("checkpoint-1")
        self.assertFalse(report["matches"])
        self.assertEqual(report["observed_size_bytes"], 7)

    def test_missing_mutated_and_symlinked_artifacts(self):
        self.register()
        self.source.unlink()
        with self.assertRaises(OSError):
            self.registry.verify("checkpoint-1")
        self.source.symlink_to(self.directory / "outside")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.registry.verify("checkpoint-1")

    def test_path_boundaries_and_regular_files(self):
        (self.root / "folder").mkdir()
        outside = self.directory / "outside"
        outside.write_text("synthetic outside")
        (self.root / "link").symlink_to(outside)
        (self.root / "linked-dir").symlink_to(self.directory, target_is_directory=True)
        for path in [
            "../outside",
            str(outside),
            "./checkpoint.bin",
            "folder/../checkpoint.bin",
            "folder//file",
            "folder",
            "link",
            "linked-dir/outside",
            "bad\\path",
            "bad\x00path",
            "",
        ]:
            with self.subTest(path=path), self.assertRaises((OSError, ValueError)):
                self.register(relative_path=path)
        if hasattr(os, "mkfifo"):
            os.mkfifo(self.root / "pipe")
            with self.assertRaisesRegex(ValueError, "regular"):
                self.register(relative_path="pipe")
        self.assertEqual(outside.read_text(), "synthetic outside")

    def test_invalid_metadata(self):
        for changes in [
            {"kind": []},
            {"kind": "unknown"},
            {"format": ""},
            {"format": "\ud800"},
            {"producer_revision": "main"},
            {"producer_revision": "A" * 40},
            {"producer_revision": 2},
        ]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.register(**changes)
        for identifier in [True, "../bad", "", "A"]:
            with self.assertRaises(ValueError):
                self.registry.register(
                    identifier,
                    relative_path="checkpoint.bin",
                    kind="checkpoint",
                    format="fixture",
                    producer_revision="a" * 40,
                )

    def test_size_limit_and_mutation_during_hashing_leave_no_record(self):
        with (
            patch("forgetrain.artifacts.MAX_BYTES", 4),
            self.assertRaisesRegex(ValueError, "64 MiB"),
        ):
            self.register()
        original_read = os.read
        changed = False

        def mutate(fd, count):
            nonlocal changed
            block = original_read(fd, count)
            if block and not changed:
                changed = True
                self.source.write_bytes(b"changed while being observed")
            return block

        with patch("forgetrain.artifacts.os.read", side_effect=mutate):
            with self.assertRaisesRegex(ValueError, "changed during hashing"):
                self.register()
        with self.assertRaises(KeyError):
            self.registry.get("checkpoint-1")

    def test_transaction_rollback_preserves_both_tables(self):
        with patch.object(
            ArtifactRegistry,
            "_audit",
            side_effect=RuntimeError("injected audit failure"),
        ):
            with self.assertRaises(RuntimeError):
                self.register()
        with self.assertRaises(KeyError):
            self.registry.get("checkpoint-1")
        with closing(sqlite3.connect(self.path)) as db, db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM registrations").fetchone()[0], 0
            )
        self.assertTrue(self.register())

    def test_concurrent_conflicting_ids_have_one_immutable_winner(self):
        (self.root / "other.bin").write_bytes(b"another synthetic artifact")
        barrier = threading.Barrier(2)

        def register(path):
            local = ArtifactRegistry(self.path)
            barrier.wait(timeout=10)
            try:
                return local.register(
                    "shared",
                    relative_path=path,
                    kind="checkpoint",
                    format="fixture",
                    producer_revision="a" * 40,
                )
            except ValueError:
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(register, ["checkpoint.bin", "other.bin"]))
        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.registry.get("shared"), winners[0])
        with closing(sqlite3.connect(self.path)) as db, db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM registrations").fetchone()[0], 1
            )

    def test_existing_database_and_root_validation(self):
        original = self.path.read_bytes()
        with self.assertRaises(FileExistsError):
            ArtifactRegistry.create(self.path, root=self.root)
        self.assertEqual(self.path.read_bytes(), original)
        (self.directory / "root-link").symlink_to(self.root, target_is_directory=True)
        for root in [
            self.directory / "missing",
            self.source,
            self.directory / "root-link",
        ]:
            path = self.directory / "new.sqlite"
            with self.assertRaises((OSError, ValueError)):
                ArtifactRegistry.create(path, root=root)
            self.assertFalse(path.exists())

    def test_registry_cannot_register_itself_or_hardlink(self):
        path = self.root / "registry.sqlite"
        registry = ArtifactRegistry.create(path, root=self.root)
        os.link(path, self.root / "alias.sqlite")
        for relative in ["registry.sqlite", "alias.sqlite"]:
            with (
                self.subTest(relative=relative),
                self.assertRaisesRegex(ValueError, "registry"),
            ):
                registry.register(
                    "db",
                    relative_path=relative,
                    kind="other",
                    format="sqlite",
                    producer_revision="a" * 40,
                )

    def test_unknown_schema_and_missing_id(self):
        with self.assertRaises(KeyError):
            self.registry.get("missing")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE registry SET version=2")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            self.register()

    def test_crashed_uncommitted_registration_rolls_back(self):
        code = """import sqlite3,sys,os
connection=sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute('BEGIN IMMEDIATE')
connection.execute(
    "INSERT INTO artifacts VALUES "
    "('crashed','checkpoint.bin','checkpoint','fixture',? ,?,1,'test')",
    ('a'*40,'b'*64))
os._exit(19)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.path)], capture_output=True
        )
        self.assertEqual(result.returncode, 19)
        with self.assertRaises(KeyError):
            ArtifactRegistry(self.path).get("crashed")
        self.assertTrue(self.register())


if __name__ == "__main__":
    unittest.main()
