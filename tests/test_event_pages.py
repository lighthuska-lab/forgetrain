import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from forgetrain.plan import Plan
from forgetrain.store import Store


class EventPageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "jobs.sqlite"
        plan = Plan.parse(
            {"version": 1, "jobs": [{"name": "job", "needs": [], "cpus": 1}]}
        )
        self.store = Store.create(self.path, plan, capacity=1)

    def populate(self):
        (lease,) = self.store.admit(now=0, lease_ms=10)
        self.store.finish(lease, outcome="failed", now=1)
        self.store.retry_failed("job", now=2, max_attempts=2)
        self.store.admit(now=3, lease_ms=10)

    def test_empty_and_persisted_pages_have_complete_order_without_mutation(self):
        empty = self.store.event_page()
        self.assertEqual(empty["events"], [])
        self.assertEqual(empty["through_seq"], 0)
        self.assertFalse(empty["has_more"])
        self.populate()
        before = self.store.snapshot()
        first = Store(self.path).event_page(limit=2)
        second = Store(self.path).event_page(
            after_seq=first["next_after_seq"], through_seq=first["through_seq"], limit=2
        )
        self.assertTrue(first["has_more"])
        self.assertFalse(second["has_more"])
        self.assertEqual(first["events"] + second["events"], before["events"])
        self.assertEqual(self.store.snapshot(), before)
        self.assertEqual(first, self.store.event_page(limit=2))

    def test_stable_prefix_excludes_new_appends_and_zero_prefix_stays_empty(self):
        self.populate()
        first = self.store.event_page(limit=2)
        old_clock = self.store.snapshot()["clock"]
        self.store.admit(now=13, lease_ms=10)
        self.assertGreater(self.store.event_page()["through_seq"], first["through_seq"])
        rest = self.store.event_page(
            after_seq=first["next_after_seq"], through_seq=first["through_seq"]
        )
        self.assertEqual([row["seq"] for row in rest["events"]], [3, 4])
        self.assertFalse(rest["has_more"])
        self.assertEqual(self.store.event_page(through_seq=0)["events"], [])
        self.assertGreater(self.store.snapshot()["clock"], old_clock)

    def test_sequence_gaps_and_cursors_beyond_prefix_do_not_repeat_events(self):
        self.populate()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM events WHERE seq=2")
            db.commit()
        first = self.store.event_page(limit=2)
        self.assertEqual([row["seq"] for row in first["events"]], [1, 3])
        second = self.store.event_page(
            after_seq=first["next_after_seq"], through_seq=first["through_seq"]
        )
        self.assertEqual([row["seq"] for row in second["events"]], [4])
        self.assertEqual(self.store.event_page(after_seq=99)["events"], [])

    def test_bounds_and_future_prefix_fail_without_mutation(self):
        self.populate()
        before = self.store.snapshot()
        for kwargs in [
            {"after_seq": True},
            {"after_seq": -1},
            {"after_seq": 2**63},
            {"limit": False},
            {"limit": 0},
            {"limit": 101},
            {"limit": 1.5},
            {"through_seq": True},
            {"through_seq": -1},
            {"through_seq": 2**63},
            {"through_seq": 5},
        ]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    self.store.event_page(**kwargs)
                self.assertEqual(self.store.snapshot(), before)

    def test_maximum_page_and_bounded_extra_row(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.executemany(
                "INSERT INTO events(job,kind,fence,clock) VALUES (?,?,?,?)",
                [("job", "synthetic", 0, 0)] * 250,
            )
            db.commit()
        first = self.store.event_page(limit=100)
        self.assertEqual(len(first["events"]), 100)
        self.assertTrue(first["has_more"])
        with self.store._connection() as db:
            plan = list(
                db.execute(
                    "EXPLAIN QUERY PLAN SELECT seq FROM events "
                    "WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?",
                    (0, 250, 101),
                )
            )
        self.assertTrue(any("INTEGER PRIMARY KEY" in row["detail"] for row in plan))

    def test_cli_reads_only_and_errors_emit_no_partial_stdout(self):
        self.populate()
        before = self.store.snapshot()
        args = [
            sys.executable,
            "-m",
            "forgetrain.state",
            "events",
            str(self.path),
            "--limit",
            "2",
        ]
        done = subprocess.run(
            args, capture_output=True, text=True, env=dict(os.environ)
        )
        self.assertEqual(done.returncode, 0, done.stderr)
        first = json.loads(done.stdout)["page"]
        self.assertEqual(len(first["events"]), 2)
        self.assertTrue(first["has_more"])
        bad = subprocess.run(
            args + ["--through-seq", "999"], capture_output=True, text=True
        )
        self.assertEqual(bad.returncode, 2)
        self.assertEqual(bad.stdout, "")
        self.assertEqual(self.store.snapshot(), before)
