import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from forgetrain.artifacts import ArtifactRegistry


class ArtifactAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.files = self.root / "files"
        self.files.mkdir()
        self.database = self.root / "registry.sqlite"
        self.registry = ArtifactRegistry.create(self.database, root=self.files)

    def register(self, identifier):
        path = self.files / identifier
        path.write_bytes(b"synthetic immutable bytes")
        self.registry.register(
            identifier,
            relative_path=identifier,
            kind="other",
            format="raw",
            producer_revision="a" * 40,
        )
        return path

    def test_actual_drift_missing_and_matching_files_account_for_every_record(self):
        self.register("a")
        self.register("b").write_bytes(b"changed")
        self.register("c").unlink()
        before = self.database.read_bytes()
        report = self.registry.audit_page()
        self.assertEqual(
            [r["status"] for r in report["results"]], ["matches", "drifted", "missing"]
        )
        self.assertEqual(sum(report["counts"].values()), 3)
        self.assertEqual(report["counts"]["missing"], 1)
        self.assertFalse(report["checkpoint_validity_verified"])
        self.assertFalse(report["complete_registry_audit"])
        self.assertNotIn(str(self.root), json.dumps(report))
        self.assertEqual(self.database.read_bytes(), before)

    def test_rejected_symlink_and_directory_do_not_stop_later_records(self):
        first = self.register("a")
        first.unlink()
        first.symlink_to(self.root / "private-missing")
        second = self.register("b")
        second.unlink()
        second.mkdir()
        self.register("c")
        report = self.registry.audit_page()
        self.assertEqual(
            [r["status"] for r in report["results"]],
            ["rejected", "rejected", "matches"],
        )
        self.assertNotIn("private-missing", json.dumps(report))

    def test_unreadable_failure_is_redacted_and_processing_continues(self):
        self.register("a")
        self.register("b")
        observe = self.registry._observe

        def observed(root, relative):
            if relative == "a":
                raise PermissionError("private-host-path-secret")
            return observe(root, relative)

        with patch.object(self.registry, "_observe", side_effect=observed):
            report = self.registry.audit_page()
        self.assertEqual(
            [r["status"] for r in report["results"]], ["unreadable", "matches"]
        )
        self.assertNotIn("private-host-path-secret", json.dumps(report))

    def test_bounded_pages_are_exclusive_and_only_selected_files_are_observed(self):
        for name in ["c", "a", "b"]:
            self.register(name)
        with patch.object(
            self.registry, "_observe", wraps=self.registry._observe
        ) as read:
            first = self.registry.audit_page(limit=2)
            self.assertEqual(read.call_count, 2)
        self.assertTrue(first["has_more"])
        self.assertEqual(first["next_after"], "b")
        second = self.registry.audit_page(limit=2, after="b")
        self.assertEqual([r["id"] for r in second["results"]], ["c"])
        self.assertFalse(second["has_more"])
        empty = self.registry.audit_page(after="z")
        self.assertEqual(empty["results"], [])
        self.assertEqual(sum(empty["counts"].values()), 0)
        for limit in [True, 0, 11, 1.5]:
            with self.assertRaises(ValueError):
                self.registry.audit_page(limit=limit)
        with self.assertRaises(ValueError):
            self.registry.audit_page(after="../bad")

    def test_size_rejection_and_mutation_failure_are_not_reported_as_drift(self):
        path = self.register("a")
        with patch("forgetrain.artifacts.MAX_BYTES", 1):
            report = self.registry.audit_page()
        self.assertEqual(report["results"][0]["status"], "rejected")
        self.assertIsNone(report["results"][0]["observed_sha256"])
        self.assertEqual(path.read_bytes(), b"synthetic immutable bytes")
        with patch.object(self.registry, "_observe", side_effect=ValueError("changed")):
            self.assertEqual(self.registry.audit_page()["counts"]["rejected"], 1)

    def test_subprocess_exit_codes_for_success_findings_and_invalid_page(self):
        path = self.register("a")
        args = [
            sys.executable,
            "-m",
            "forgetrain.artifact_cli",
            "audit",
            str(self.database),
        ]
        success = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(success.returncode, 0, success.stderr)
        self.assertEqual(json.loads(success.stdout)["audit"]["counts"]["matches"], 1)
        path.unlink()
        missing = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(missing.returncode, 1)
        self.assertEqual(json.loads(missing.stdout)["audit"]["counts"]["missing"], 1)
        invalid = subprocess.run(
            args + ["--limit", "11"], capture_output=True, text=True
        )
        self.assertEqual(invalid.returncode, 2)
        self.assertEqual(invalid.stdout, "")
