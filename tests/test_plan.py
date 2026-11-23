import contextlib
import copy
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from forgetrain.__main__ import main
from forgetrain.plan import Plan


def job(name, needs=(), cpus=1):
    return {"name": name, "needs": list(needs), "cpus": cpus}


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.document = {
            "version": 1,
            "jobs": [job("data"), job("train", ["data"], 2), job("eval", ["train"])],
        }
        self.plan = Plan.parse(self.document)

    def test_dependency_progression(self):
        states = {"data": "pending", "train": "pending", "eval": "pending"}
        self.assertEqual(self.plan.select(states, free_cpus=4), ("data",))
        states["data"] = "succeeded"
        self.assertEqual(self.plan.select(states, free_cpus=1), ())
        self.assertEqual(self.plan.select(states, free_cpus=2), ("train",))
        states["train"] = "succeeded"
        self.assertEqual(self.plan.select(states, free_cpus=1), ("eval",))

    def test_failure_cancellation_and_running_do_not_release_dependents(self):
        for state in ("failed", "cancelled", "running"):
            states = {"data": state, "train": "pending", "eval": "pending"}
            self.assertEqual(self.plan.select(states, free_cpus=100), ())

    def test_capacity_packing_and_determinism(self):
        plan = Plan.parse(
            {"version": 1, "jobs": [job("a", cpus=3), job("b"), job("c")]}
        )
        states = dict.fromkeys(("a", "b", "c"), "pending")
        before = states.copy()
        self.assertEqual(plan.select(states, free_cpus=2), ("b", "c"))
        self.assertEqual(plan.select(states, free_cpus=3), ("a",))
        self.assertEqual(plan.select(states, free_cpus=0), ())
        self.assertEqual(states, before)

    def test_schema_rejects_unknown_and_invalid_fields(self):
        bad = [
            None,
            [],
            {},
            {"version": True, "jobs": [job("a")]},
            {"version": 2, "jobs": [job("a")]},
            {"version": 1, "jobs": []},
        ]
        for row in [
            job("A"),
            job("a", cpus=True),
            job("a", cpus=0),
            job("a", cpus=1025),
            job("a", ["b", "b"]),
            {"name": "a", "needs": "b", "cpus": 1},
            {**job("a"), "command": "echo unsafe"},
        ]:
            bad.append({"version": 1, "jobs": [row]})
        for document in bad:
            with self.subTest(document=document), self.assertRaises(ValueError):
                Plan.parse(document)

    def test_duplicate_unknown_and_cyclic_dependencies(self):
        for rows in (
            [job("a"), job("a")],
            [job("a", ["missing"])],
            [job("a", ["a"])],
            [job("a", ["b"]), job("b", ["a"])],
        ):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                Plan.parse({"version": 1, "jobs": rows})

    def test_long_reverse_declared_dag_does_not_recurse(self):
        rows = [job(f"j{i}", [f"j{i - 1}"] if i else []) for i in range(1000)]
        plan = Plan.parse({"version": 1, "jobs": rows[::-1]})
        self.assertEqual(
            plan.select(
                dict.fromkeys((j.name for j in plan.jobs), "pending"), free_cpus=1000
            ),
            ("j0",),
        )

    def test_inconsistent_states_and_invalid_capacity(self):
        valid = dict.fromkeys(("data", "train", "eval"), "pending")
        for states in (
            {},
            {**valid, "extra": "pending"},
            {**valid, "data": []},
            {**valid, "data": "unknown"},
            {**valid, "train": "running"},
            {**valid, "eval": "succeeded"},
        ):
            with self.subTest(states=states), self.assertRaises(ValueError):
                self.plan.select(states, free_cpus=4)
        for cpus in (True, -1, 1.5):
            with self.assertRaises(ValueError):
                self.plan.select(valid, free_cpus=cpus)

    def test_parse_detaches_mutable_input(self):
        document = copy.deepcopy(self.document)
        plan = Plan.parse(document)
        document["jobs"][1]["needs"].clear()
        self.assertEqual(plan.jobs[1].needs, ("data",))

    def test_input_caps(self):
        with self.assertRaises(ValueError):
            Plan.parse({"version": 1, "jobs": [job(f"j{i}") for i in range(1001)]})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "huge.json"
            path.write_bytes(b" " * (1024 * 1024 + 1))
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(main([str(path), "--free-cpus", "1"]), 2)

    def test_cli_real_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            path.write_text(json.dumps(self.document))
            result = subprocess.run(
                [sys.executable, "-m", "forgetrain", str(path), "--free-cpus", "2"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                json.loads(result.stdout), {"version": 1, "ready": ["data"], "jobs": 3}
            )

    def test_cli_bad_input_has_no_partial_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            for value in ('{"version":1,"version":2,"jobs":[]}', "invalid", "[]"):
                path.write_text(value)
                out, err = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    self.assertEqual(main([str(path), "--free-cpus", "2"]), 2)
                self.assertEqual(out.getvalue(), "")
                self.assertNotIn("Traceback", err.getvalue())


if __name__ == "__main__":
    unittest.main()
