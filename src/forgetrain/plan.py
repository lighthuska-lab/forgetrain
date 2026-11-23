"""Validate a declarative DAG and select ready jobs without executing code."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Job:
    name: str
    needs: tuple[str, ...]
    cpus: int


@dataclass(frozen=True)
class Plan:
    jobs: tuple[Job, ...]

    @classmethod
    def parse(cls, document: Any) -> Plan:
        if not isinstance(document, dict) or set(document) != {"version", "jobs"}:
            raise ValueError("plan requires exactly version and jobs")
        if type(document["version"]) is not int or document["version"] != 1:
            raise ValueError("unsupported plan version")
        rows = document["jobs"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
            raise ValueError("plan requires 1 to 1000 jobs")
        jobs = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"name", "needs", "cpus"}:
                raise ValueError("job requires exactly name, needs and cpus")
            name, needs, cpus = row["name"], row["needs"], row["cpus"]
            if not isinstance(name, str) or not re.fullmatch(
                r"[a-z][a-z0-9_-]{0,63}", name
            ):
                raise ValueError("invalid job name")
            if not isinstance(needs, list) or any(
                not isinstance(n, str) for n in needs
            ):
                raise ValueError("needs must be a list of job names")
            if len(set(needs)) != len(needs):
                raise ValueError("duplicate dependency")
            if type(cpus) is not int or not 1 <= cpus <= 1024:
                raise ValueError("cpus must be an integer from 1 to 1024")
            jobs.append(Job(name, tuple(needs), cpus))
        names = {j.name for j in jobs}
        if len(names) != len(jobs):
            raise ValueError("duplicate job name")
        for job in jobs:
            if any(n not in names for n in job.needs):
                raise ValueError("unknown dependency")
        # Iterative traversal supports long DAGs without Python recursion limits.
        pending = list(jobs)
        visited: set[str] = set()
        while pending:
            ready = [j for j in pending if set(j.needs) <= visited]
            if not ready:
                raise ValueError("dependency cycle")
            visited.update(j.name for j in ready)
            pending = [j for j in pending if j.name not in visited]
        return cls(tuple(jobs))

    def select(self, states: dict[str, str], *, free_cpus: int) -> tuple[str, ...]:
        """Return one deterministic admission batch in declaration order.

        Capacity is already free of running work; this pure function reserves
        nothing. A future persistent scheduler must transact admission separately.
        """
        if type(free_cpus) is not int or free_cpus < 0:
            raise ValueError("free_cpus must be a nonnegative integer")
        names = {j.name for j in self.jobs}
        allowed = {"pending", "running", "succeeded", "failed", "cancelled"}
        if not isinstance(states, dict) or set(states) != names:
            raise ValueError("states must cover exactly the plan jobs")
        if any(not isinstance(s, str) or s not in allowed for s in states.values()):
            raise ValueError("unknown job state")
        for job in self.jobs:
            if states[job.name] in {"running", "succeeded"} and any(
                states[n] != "succeeded" for n in job.needs
            ):
                raise ValueError("started job has unsatisfied dependencies")
        selected = []
        for job in self.jobs:
            if (
                states[job.name] == "pending"
                and all(states[n] == "succeeded" for n in job.needs)
                and job.cpus <= free_cpus
            ):
                selected.append(job.name)
                free_cpus -= job.cpus
        return tuple(selected)
