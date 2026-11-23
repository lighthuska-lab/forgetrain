import concurrent.futures
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from forgetrain.plan import Plan
from forgetrain.store import Lease, Store


def plan(independent=False):
    return Plan.parse(
        {
            "version": 1,
            "jobs": [
                {"name": "prepare", "needs": [], "cpus": 2},
                {
                    "name": "train",
                    "needs": [] if independent else ["prepare"],
                    "cpus": 2,
                },
            ],
        }
    )


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "jobs.sqlite"
        self.store = Store.create(self.path, plan(), capacity=2)

    def test_dependency_progress_and_durable_restart(self):
        (first,) = self.store.admit(now=0, lease_ms=10)
        self.assertEqual(first.job, "prepare")
        self.assertEqual(Store(self.path).admit(now=1, lease_ms=10), ())
        Store(self.path).finish(first, outcome="succeeded", now=2)
        (second,) = Store(self.path).admit(now=3, lease_ms=10)
        self.assertEqual(second.job, "train")
        Store(self.path).finish(second, outcome="succeeded", now=4)
        report = self.store.snapshot()
        self.assertEqual(
            [j["state"] for j in report["jobs"]], ["succeeded", "succeeded"]
        )
        self.assertEqual(
            [event["kind"] for event in report["events"]],
            ["admitted", "succeeded", "admitted", "succeeded"],
        )

    def test_real_connections_cannot_double_admit_or_oversubscribe(self):
        path = Path(self.temp.name) / "concurrent.sqlite"
        Store.create(path, plan(True), capacity=2)
        barrier = threading.Barrier(8)

        def admit(_):
            local = Store(path)
            barrier.wait(timeout=10)
            return local.admit(now=0, lease_ms=100)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(admit, range(8)))
        self.assertEqual(sum(len(batch) for batch in results), 1)
        jobs = Store(path).snapshot()["jobs"]
        self.assertEqual(sum(j["cpus"] for j in jobs if j["state"] == "running"), 2)

    def test_expiry_reclaims_capacity_and_fences_stale_receipts(self):
        (old,) = self.store.admit(now=10, lease_ms=10)
        (fresh,) = Store(self.path).admit(now=20, lease_ms=10)
        self.assertEqual(fresh.job, old.job)
        self.assertGreater(fresh.fence, old.fence)
        before = self.store.snapshot()
        for outcome in ["succeeded", "failed", "cancelled"]:
            with (
                self.subTest(outcome=outcome),
                self.assertRaisesRegex(ValueError, "stale"),
            ):
                self.store.finish(old, outcome=outcome, now=21)
            self.assertEqual(self.store.snapshot(), before)
        self.store.finish(fresh, outcome="succeeded", now=22)

    def test_exact_deadline_cannot_finish(self):
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.store.finish(receipt, outcome="succeeded", now=10)
        (fresh,) = self.store.admit(now=10, lease_ms=5)
        self.assertEqual(fresh.fence, 2)

    def test_failed_and_cancelled_jobs_block_descendants(self):
        for outcome in ["failed", "cancelled"]:
            path = Path(self.temp.name) / (outcome + ".sqlite")
            store = Store.create(path, plan(), capacity=2)
            (receipt,) = store.admit(now=0, lease_ms=10)
            store.finish(receipt, outcome=outcome, now=1)
            self.assertEqual(store.admit(now=2, lease_ms=10), ())
        self.store.cancel_pending("prepare", now=0)
        self.assertEqual(self.store.admit(now=1, lease_ms=10), ())

    def test_admission_and_finish_rollback_have_no_partial_state(self):
        before = self.store.snapshot()
        with patch.object(Store, "_event", side_effect=RuntimeError("injected fault")):
            with self.assertRaises(RuntimeError):
                self.store.admit(now=10, lease_ms=10)
        self.assertEqual(self.store.snapshot(), before)
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        before = self.store.snapshot()
        with patch.object(Store, "_event", side_effect=RuntimeError("injected fault")):
            with self.assertRaises(RuntimeError):
                self.store.finish(receipt, outcome="succeeded", now=1)
        self.assertEqual(self.store.snapshot(), before)

    def test_process_crash_rolls_back_uncommitted_admission(self):
        code = """import sqlite3,sys,os
connection=sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute('BEGIN IMMEDIATE')
connection.execute("UPDATE jobs SET state='running', deadline=999 WHERE name='prepare'")
os._exit(19)
"""
        before = self.store.snapshot()
        child = subprocess.run(
            [sys.executable, "-c", code, str(self.path)], capture_output=True
        )
        self.assertEqual(child.returncode, 19)
        self.assertEqual(Store(self.path).snapshot(), before)
        self.assertEqual(len(self.store.admit(now=0, lease_ms=10)), 1)

    def test_invalid_time_parameters_and_receipts(self):
        for now in [True, -1, 1.5, 2**53 + 1]:
            with self.assertRaises(ValueError):
                self.store.admit(now=now, lease_ms=10)
        for duration in [True, 0, 86400001]:
            with self.assertRaises(ValueError):
                self.store.admit(now=0, lease_ms=duration)
        (receipt,) = self.store.admit(now=5, lease_ms=10)
        for fake in [
            Lease("prepare", receipt.fence + 1, receipt.expires),
            Lease("prepare", receipt.fence, receipt.expires + 1),
            Lease("unknown", 1, 15),
            Lease("../bad", 1, 15),
        ]:
            with self.assertRaises(ValueError):
                self.store.finish(fake, outcome="succeeded", now=6)
        with self.assertRaises(ValueError):
            self.store.admit(now=4, lease_ms=10)
        with self.assertRaises(ValueError):
            self.store.finish(receipt, outcome="running", now=6)
        with self.assertRaises(ValueError):
            self.store.cancel_pending("prepare", now=6)

    def test_existing_database_never_overwritten_and_missing_not_created(self):
        original = self.path.read_bytes()
        with self.assertRaises(FileExistsError):
            Store.create(self.path, plan(), capacity=2)
        self.assertEqual(self.path.read_bytes(), original)
        missing = Path(self.temp.name) / "missing.sqlite"
        with self.assertRaises(FileNotFoundError):
            Store(missing)
        self.assertFalse(missing.exists())

    def test_unknown_schema_fails_closed(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE meta SET version=2")
        with self.assertRaises(ValueError):
            self.store.admit(now=0, lease_ms=10)


if __name__ == "__main__":
    unittest.main()
