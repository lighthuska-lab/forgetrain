import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from forgetrain.store import Store


class StateCLITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.database = self.root / "state.sqlite"
        self.plan = self.root / "plan.json"
        self.receipt = self.root / "receipt.json"
        self.plan.write_text(
            json.dumps(
                {
                    "version": 1,
                    "jobs": [
                        {"name": "prepare", "needs": [], "cpus": 1},
                        {"name": "train", "needs": ["prepare"], "cpus": 1},
                    ],
                }
            )
        )

    def command(self, *args, code=0):
        result = subprocess.run(
            [sys.executable, "-m", "forgetrain.state", *map(str, args)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, code, result.stderr)
        if code:
            self.assertEqual(result.stdout, "")
            self.assertTrue(result.stderr)
            return None
        self.assertEqual(result.stderr, "")
        value = json.loads(result.stdout)
        self.assertEqual(value["schema_version"], 1)
        return value

    def create(self):
        return self.command("create", self.database, self.plan, "--capacity", 1)

    def admit(self, now=0, duration=10):
        return self.command(
            "admit", self.database, "--now", now, "--lease-ms", duration
        )

    def save(self, receipt):
        self.receipt.write_text(json.dumps(receipt))

    def test_persisted_create_admit_renew_finish_and_dependency_progress(self):
        created = self.create()
        self.assertEqual(created["state"]["jobs"][0]["state"], "pending")
        old = self.admit()["leases"][0]
        self.assertEqual(
            old, {"schema_version": 1, "job": "prepare", "fence": 1, "expires": 10}
        )
        self.save(old)
        renewed = self.command(
            "renew",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            3,
            "--lease-ms",
            20,
        )["receipt"]
        self.assertEqual(renewed["expires"], 23)
        before = Store(self.database).snapshot()
        self.command(
            "finish",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            4,
            "--outcome",
            "succeeded",
            code=2,
        )
        self.assertEqual(Store(self.database).snapshot(), before)
        self.save(renewed)
        self.command(
            "finish",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            4,
            "--outcome",
            "succeeded",
        )
        next_lease = self.admit(now=5)["leases"][0]
        self.assertEqual(next_lease["job"], "train")
        status = self.command("status", self.database)["state"]
        self.assertEqual([j["state"] for j in status["jobs"]], ["succeeded", "running"])

    def test_expiry_recovery_and_backwards_clock_fail_without_partial_state(self):
        self.create()
        old = self.admit()["leases"][0]
        self.save(old)
        self.command(
            "renew",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            10,
            "--lease-ms",
            20,
            code=2,
        )
        fresh = self.admit(now=10)["leases"][0]
        self.assertGreater(fresh["fence"], old["fence"])
        before = Store(self.database).snapshot()
        self.command("admit", self.database, "--now", 9, "--lease-ms", 5, code=2)
        self.command(
            "finish",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            11,
            "--outcome",
            "failed",
            code=2,
        )
        self.assertEqual(Store(self.database).snapshot(), before)

    def test_failure_cancellation_and_pending_cancel_block_descendants(self):
        for outcome in ["failed", "cancelled"]:
            self.database = self.root / (outcome + ".sqlite")
            self.create()
            self.save(self.admit()["leases"][0])
            self.command(
                "finish",
                self.database,
                "--receipt",
                self.receipt,
                "--now",
                1,
                "--outcome",
                outcome,
            )
            self.assertEqual(self.admit(now=2)["leases"], [])
        self.database = self.root / "pending.sqlite"
        self.create()
        self.command("cancel", self.database, "prepare", "--now", 0)
        self.assertEqual(self.admit(now=1)["leases"], [])

    def test_invalid_receipt_schema_and_json_do_not_mutate_database(self):
        self.create()
        valid = self.admit()["leases"][0]
        before = Store(self.database).snapshot()
        malformed = [
            dict(valid, schema_version=True),
            dict(valid, schema_version=2),
            dict(valid, extra="no"),
            dict(valid, fence=True),
            dict(valid, expires=1.5),
            dict(valid, job="../bad"),
            {},
        ]
        for record in malformed:
            self.save(record)
            self.command(
                "finish",
                self.database,
                "--receipt",
                self.receipt,
                "--now",
                1,
                "--outcome",
                "succeeded",
                code=2,
            )
        for raw in [
            b"\xff",
            b'{"schema_version":1,"schema_version":1}',
            b'{"x":NaN}',
            b"[" * 2000 + b"]" * 2000,
            b" " * (16 * 1024 + 1),
        ]:
            self.receipt.write_bytes(raw)
            self.command(
                "renew",
                self.database,
                "--receipt",
                self.receipt,
                "--now",
                1,
                "--lease-ms",
                20,
                code=2,
            )
        self.assertEqual(Store(self.database).snapshot(), before)

    def test_exclusive_create_and_invalid_plan_preserve_existing_data(
        self,
    ):
        self.create()
        original = self.database.read_bytes()
        self.command("create", self.database, self.plan, "--capacity", 1, code=2)
        self.assertEqual(self.database.read_bytes(), original)
        self.database = self.root / "invalid.sqlite"
        for raw in [
            b"{}",
            b'{"version":NaN,"jobs":[]}',
            b" " * (1024 * 1024 + 1),
            b"\xff",
        ]:
            self.plan.write_bytes(raw)
            self.command("create", self.database, self.plan, "--capacity", 1, code=2)
            self.assertFalse(self.database.exists())

    def test_bounded_status_includes_last_events_without_mutation(self):
        self.create()
        self.save(self.admit()["leases"][0])
        renewed = self.command(
            "renew",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            1,
            "--lease-ms",
            20,
        )["receipt"]
        self.save(renewed)
        self.command(
            "finish",
            self.database,
            "--receipt",
            self.receipt,
            "--now",
            2,
            "--outcome",
            "succeeded",
        )
        before = Store(self.database).snapshot()
        for limit in [0, 1, 2, 1000]:
            state = self.command("status", self.database, "--event-limit", limit)[
                "state"
            ]
            self.assertEqual(state["event_count"], 3)
            self.assertEqual(len(state["events"]), min(limit, 3))
            self.assertEqual(state["events_truncated"], limit < 3)
            self.assertEqual(
                [e["seq"] for e in state["events"]], list(range(4 - min(limit, 3), 4))
            )
        self.command("status", self.database, "--event-limit", -1, code=2)
        self.command("status", self.database, "--event-limit", 1001, code=2)
        self.assertEqual(Store(self.database).snapshot(), before)

    def test_nonregular_inputs_and_missing_or_corrupt_database(self):
        self.create()
        if hasattr(os, "mkfifo"):
            pipe = self.root / "pipe"
            os.mkfifo(pipe)
            self.command(
                "finish",
                self.database,
                "--receipt",
                pipe,
                "--now",
                0,
                "--outcome",
                "succeeded",
                code=2,
            )
        self.command(
            "admit", self.root / "missing.sqlite", "--now", 0, "--lease-ms", 10, code=2
        )
        corrupt = self.root / "corrupt.sqlite"
        corrupt.write_bytes(b"not sqlite")
        self.command("status", corrupt, code=2)
        self.assertEqual(corrupt.read_bytes(), b"not sqlite")

    def test_original_plan_cli_still_works(self):
        result = subprocess.run(
            [sys.executable, "-m", "forgetrain", str(self.plan), "--free-cpus", "1"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["ready"], ["prepare"])


if __name__ == "__main__":
    unittest.main()
