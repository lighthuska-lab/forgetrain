import concurrent.futures
import os
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

    def test_explicit_failed_retry_preserves_fence_capacity_and_dependencies(self):
        (old,) = self.store.admit(now=0, lease_ms=10)
        self.store.finish(old, outcome="failed", now=1)
        self.assertEqual(self.store.admit(now=2, lease_ms=10), ())
        Store(self.path).retry_failed("prepare", now=3, max_attempts=2)
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["jobs"][0]["state"], "pending")
        self.assertEqual(snapshot["jobs"][0]["fence"], 1)
        self.assertEqual(snapshot["events"][-1]["kind"], "retry_requested")
        (fresh,) = Store(self.path).admit(now=4, lease_ms=10)
        self.assertEqual(fresh.fence, 2)
        before = self.store.snapshot()
        with self.assertRaises(ValueError):
            self.store.finish(old, outcome="succeeded", now=5)
        self.assertEqual(self.store.snapshot(), before)
        self.store.finish(fresh, outcome="succeeded", now=5)
        (dependent,) = self.store.admit(now=6, lease_ms=10)
        self.assertEqual(dependent.job, "train")

    def test_retry_limit_invalid_options_and_terminal_states_do_not_mutate(self):
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        self.store.finish(receipt, outcome="failed", now=1)
        before = self.store.snapshot()
        for limit in (True, 0, -1, 1001, 1.5, "2", 1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.store.retry_failed("prepare", now=2, max_attempts=limit)
            self.assertEqual(self.store.snapshot(), before)
        for name in ("train", "missing", "../prepare", None):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.store.retry_failed(name, now=2, max_attempts=2)
            self.assertEqual(self.store.snapshot(), before)
        self.store.retry_failed("prepare", now=2, max_attempts=2)
        (second,) = self.store.admit(now=3, lease_ms=10)
        self.store.finish(second, outcome="failed", now=4)
        before = self.store.snapshot()
        with self.assertRaises(ValueError):
            self.store.retry_failed("prepare", now=5, max_attempts=2)
        self.assertEqual(self.store.snapshot(), before)

    def test_concurrent_retry_requests_requeue_only_once(self):
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        self.store.finish(receipt, outcome="failed", now=1)
        barrier = threading.Barrier(4)

        def retry(_):
            barrier.wait(timeout=10)
            try:
                Store(self.path).retry_failed("prepare", now=2, max_attempts=2)
                return True
            except ValueError:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(retry, range(4))), 1)
        events = self.store.snapshot()["events"]
        self.assertEqual(sum(event["kind"] == "retry_requested" for event in events), 1)

    def test_rejected_retry_rolls_back_other_job_expiry_and_clock(self):
        path = Path(self.temp.name) / "independent.sqlite"
        store = Store.create(path, plan(True), capacity=4)
        receipts = store.admit(now=0, lease_ms=10)
        store.finish(receipts[0], outcome="failed", now=1)
        before = store.snapshot()
        with self.assertRaises(ValueError):
            store.retry_failed(receipts[0].job, now=10, max_attempts=1)
        self.assertEqual(store.snapshot(), before)
        store.retry_failed(receipts[0].job, now=10, max_attempts=2)
        self.assertTrue(
            all(job["state"] == "pending" for job in store.snapshot()["jobs"])
        )

    def test_relative_store_remains_bound_when_working_directory_changes(self):
        original_cwd = Path.cwd()
        first = Path(self.temp.name) / "first"
        second = Path(self.temp.name) / "second"
        first.mkdir()
        second.mkdir()
        other = Store.create(second / "jobs.sqlite", plan(), capacity=4)
        try:
            os.chdir(first)
            created = Store.create("jobs.sqlite", plan(), capacity=2)
            reopened = Store("jobs.sqlite")
            os.chdir(second)
            for bound in (created, reopened):
                self.assertEqual(bound.snapshot()["capacity"], 2)
            (receipt,) = reopened.admit(now=0, lease_ms=10)
            self.assertEqual(receipt.job, "prepare")
            self.assertEqual(other.snapshot()["events"], [])
            self.assertEqual(
                Store(first / "jobs.sqlite").snapshot()["events"][0]["kind"], "admitted"
            )
        finally:
            os.chdir(original_cwd)

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

    def test_renewal_survives_restart_preserves_capacity_and_invalidates_old_receipt(
        self,
    ):
        (old,) = self.store.admit(now=0, lease_ms=10)
        fresh = Store(self.path).renew(old, now=5, lease_ms=20)
        self.assertEqual(fresh, Lease(old.job, old.fence, 25))
        self.assertEqual(Store(self.path).admit(now=11, lease_ms=10), ())
        for operation in [
            lambda: self.store.finish(old, outcome="succeeded", now=12),
            lambda: self.store.renew(old, now=12, lease_ms=30),
        ]:
            before = self.store.snapshot()
            with self.assertRaisesRegex(ValueError, "stale"):
                operation()
            self.assertEqual(self.store.snapshot(), before)
        Store(self.path).finish(fresh, outcome="succeeded", now=13)
        self.assertEqual(
            [event["kind"] for event in self.store.snapshot()["events"]],
            ["admitted", "renewed", "succeeded"],
        )

    def test_renewal_cannot_shorten_or_keep_deadline(self):
        (old,) = self.store.admit(now=0, lease_ms=10)
        for duration in [1, 5]:
            before = self.store.snapshot()
            with self.assertRaisesRegex(ValueError, "strictly extend"):
                self.store.renew(old, now=5, lease_ms=duration)
            self.assertEqual(self.store.snapshot(), before)

    def test_renewal_at_expiry_or_after_terminal_state_fails(self):
        (old,) = self.store.admit(now=0, lease_ms=10)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.store.renew(old, now=10, lease_ms=20)
        (new,) = self.store.admit(now=10, lease_ms=10)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.store.renew(old, now=11, lease_ms=20)
        self.store.finish(new, outcome="cancelled", now=12)
        with self.assertRaisesRegex(ValueError, "stale"):
            self.store.renew(new, now=13, lease_ms=20)

    def test_concurrent_renewals_have_exactly_one_winner(self):
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        barrier = threading.Barrier(6)

        def renew(_):
            local = Store(self.path)
            barrier.wait(timeout=10)
            try:
                return local.renew(receipt, now=5, lease_ms=20)
            except ValueError:
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(renew, range(6)))
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(self.store.snapshot()["jobs"][0]["deadline"], 25)
        self.assertEqual(
            [event["kind"] for event in self.store.snapshot()["events"]],
            ["admitted", "renewed"],
        )

    def test_concurrent_renewal_and_completion_cannot_both_succeed(self):
        (receipt,) = self.store.admit(now=0, lease_ms=10)
        barrier = threading.Barrier(2)

        def act(renew):
            local = Store(self.path)
            barrier.wait(timeout=10)
            try:
                if renew:
                    local.renew(receipt, now=5, lease_ms=20)
                else:
                    local.finish(receipt, outcome="succeeded", now=5)
                return True
            except ValueError:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(act, [True, False])), 1)
        self.assertEqual(len(self.store.snapshot()["events"]), 2)

    def test_renewal_fault_rolls_back_clock_deadline_and_event(self):
        (old,) = self.store.admit(now=0, lease_ms=10)
        before = self.store.snapshot()
        with patch.object(Store, "_event", side_effect=RuntimeError("injected fault")):
            with self.assertRaises(RuntimeError):
                self.store.renew(old, now=5, lease_ms=20)
        self.assertEqual(self.store.snapshot(), before)
        self.store.finish(old, outcome="succeeded", now=1)

    def test_renewal_validates_duration_receipt_and_clock(self):
        (old,) = self.store.admit(now=5, lease_ms=10)
        before = self.store.snapshot()
        for duration in [True, 0, 86400001]:
            with self.assertRaises(ValueError):
                self.store.renew(old, now=6, lease_ms=duration)
        for receipt in [
            None,
            Lease("../bad", 1, 15),
            Lease("prepare", True, 15),
            Lease("prepare", 1, False),
            Lease("unknown", 1, 15),
        ]:
            with self.assertRaises(ValueError):
                self.store.renew(receipt, now=6, lease_ms=20)
        for now in [True, -1, 4, 2**53 + 1]:
            with self.assertRaises(ValueError):
                self.store.renew(old, now=now, lease_ms=20)
        self.assertEqual(self.store.snapshot(), before)

    def test_unknown_schema_fails_closed(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE meta SET version=2")
        with self.assertRaises(ValueError):
            self.store.admit(now=0, lease_ms=10)


if __name__ == "__main__":
    unittest.main()
