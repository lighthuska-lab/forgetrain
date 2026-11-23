"""Single-host transactional reservations; receipts fence stale cooperative workers."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .plan import Plan


@dataclass(frozen=True)
class Lease:
    job: str
    fence: int
    expires: int


def _integer(value: Any, label: str, low: int, high: int) -> None:
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"invalid {label}")


class Store:
    """One fixed plan/capacity per trusted local SQLite database.

    Mutations require nondecreasing caller time in integer milliseconds. A lease
    expires exactly at its deadline; expiry requeues the job. Fencing protects
    store mutations, not external artifacts or a process still running after expiry.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not self.path.is_file():
            raise FileNotFoundError(self.path)

    @classmethod
    def create(cls, path: str | Path, plan: Plan, *, capacity: int) -> Store:
        _integer(capacity, "capacity", 1, 1024)
        document = {
            "version": 1,
            "jobs": [
                {"name": j.name, "needs": list(j.needs), "cpus": j.cpus}
                for j in plan.jobs
            ],
        }
        plan = Plan.parse(document)
        target = Path(path)
        # Exclusive creation prevents accidentally replacing an existing scheduler.
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        store = cls(target)
        with store._transaction() as db:
            db.execute(
                "CREATE TABLE meta (version INTEGER, capacity INTEGER, clock INTEGER)"
            )
            db.execute("INSERT INTO meta VALUES (1, ?, 0)", (capacity,))
            db.execute("""CREATE TABLE jobs (
                name TEXT PRIMARY KEY, ordinal INTEGER UNIQUE NOT NULL,
                needs TEXT NOT NULL, cpus INTEGER NOT NULL CHECK(cpus > 0),
                state TEXT NOT NULL CHECK(state IN
                    ('pending','running','succeeded','failed','cancelled')),
                fence INTEGER NOT NULL CHECK(fence >= 0), deadline INTEGER,
                CHECK ((state='running') = (deadline IS NOT NULL)))""")
            db.execute("""CREATE TABLE events (
                seq INTEGER PRIMARY KEY, job TEXT NOT NULL, kind TEXT NOT NULL,
                fence INTEGER NOT NULL, clock INTEGER NOT NULL)""")
            for position, job in enumerate(plan.jobs):
                db.execute(
                    "INSERT INTO jobs VALUES (?, ?, ?, ?, 'pending', 0, NULL)",
                    (job.name, position, json.dumps(job.needs), job.cpus),
                )
        return store

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        # Read-write URI mode refuses a missing DB instead of creating one.
        db = sqlite3.connect(
            self.path.resolve().as_uri() + "?mode=rw",
            uri=True,
            timeout=5,
            isolation_level=None,
        )
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise

    def _advance(self, db: sqlite3.Connection, now: int) -> None:
        _integer(now, "clock", 0, 2**53)
        metadata = db.execute("SELECT * FROM meta").fetchone()
        if metadata is None or metadata["version"] != 1:
            raise ValueError("unsupported store schema")
        if now < metadata["clock"]:
            raise ValueError("clock must not move backwards")
        db.execute("UPDATE meta SET clock=?", (now,))
        for row in db.execute(
            "SELECT * FROM jobs WHERE state='running' AND deadline<=?", (now,)
        ).fetchall():
            db.execute(
                "UPDATE jobs SET state='pending', deadline=NULL WHERE name=?",
                (row["name"],),
            )
            self._event(db, row["name"], "expired", row["fence"], now)

    @staticmethod
    def _event(
        db: sqlite3.Connection, job: str, kind: str, fence: int, now: int
    ) -> None:
        db.execute(
            "INSERT INTO events(job,kind,fence,clock) VALUES (?,?,?,?)",
            (job, kind, fence, now),
        )

    def admit(self, *, now: int, lease_ms: int) -> tuple[Lease, ...]:
        _integer(lease_ms, "lease duration", 1, 86400000)
        with self._transaction() as db:
            self._advance(db, now)
            rows = db.execute("SELECT * FROM jobs ORDER BY ordinal").fetchall()
            capacity = db.execute("SELECT capacity FROM meta").fetchone()[0]
            free = capacity - sum(
                row["cpus"] for row in rows if row["state"] == "running"
            )
            if free < 0:
                raise ValueError("store capacity invariant violated")
            plan = Plan.parse(
                {
                    "version": 1,
                    "jobs": [
                        {
                            "name": row["name"],
                            "needs": json.loads(row["needs"]),
                            "cpus": row["cpus"],
                        }
                        for row in rows
                    ],
                }
            )
            selected = plan.select(
                {row["name"]: row["state"] for row in rows}, free_cpus=free
            )
            leases = []
            for name in selected:
                db.execute(
                    "UPDATE jobs SET state='running', fence=fence+1, deadline=? "
                    "WHERE name=?",
                    (now + lease_ms, name),
                )
                fence = db.execute(
                    "SELECT fence FROM jobs WHERE name=?", (name,)
                ).fetchone()[0]
                self._event(db, name, "admitted", fence, now)
                leases.append(Lease(name, fence, now + lease_ms))
            return tuple(leases)

    @staticmethod
    def _validate_receipt(lease: Lease) -> None:
        if not isinstance(lease, Lease):
            raise ValueError("a lease receipt is required")
        if not isinstance(lease.job, str) or not re.fullmatch(
            r"[a-z][a-z0-9_-]{0,63}", lease.job
        ):
            raise ValueError("invalid receipt job name")
        _integer(lease.fence, "fence", 1, 2**63 - 1)
        _integer(lease.expires, "deadline", 1, 2**54)

    def finish(self, lease: Lease, *, outcome: str, now: int) -> None:
        if not isinstance(outcome, str) or outcome not in {
            "succeeded",
            "failed",
            "cancelled",
        }:
            raise ValueError("invalid terminal outcome")
        self._validate_receipt(lease)
        with self._transaction() as db:
            self._advance(db, now)
            row = db.execute("SELECT * FROM jobs WHERE name=?", (lease.job,)).fetchone()
            if (
                row is None
                or row["state"] != "running"
                or row["fence"] != lease.fence
                or row["deadline"] != lease.expires
            ):
                raise ValueError("stale or invalid lease receipt")
            db.execute(
                "UPDATE jobs SET state=?, deadline=NULL WHERE name=?",
                (outcome, lease.job),
            )
            self._event(db, lease.job, outcome, lease.fence, now)

    def renew(self, lease: Lease, *, now: int, lease_ms: int) -> Lease:
        """Extend one live receipt atomically; the previous deadline becomes stale.

        The attempt fence is unchanged. The replacement deadline must strictly
        exceed the current deadline, and is measured from caller time, not added
        to the old deadline. A receipt at exact expiry cannot be renewed.
        """
        self._validate_receipt(lease)
        _integer(lease_ms, "lease duration", 1, 86400000)
        with self._transaction() as db:
            self._advance(db, now)
            row = db.execute("SELECT * FROM jobs WHERE name=?", (lease.job,)).fetchone()
            if (
                row is None
                or row["state"] != "running"
                or row["fence"] != lease.fence
                or row["deadline"] != lease.expires
            ):
                raise ValueError("stale or invalid lease receipt")
            deadline = now + lease_ms
            if deadline <= lease.expires:
                raise ValueError("renewal must strictly extend the deadline")
            db.execute("UPDATE jobs SET deadline=? WHERE name=?", (deadline, lease.job))
            self._event(db, lease.job, "renewed", lease.fence, now)
            return Lease(lease.job, lease.fence, deadline)

    def cancel_pending(self, name: str, *, now: int) -> None:
        with self._transaction() as db:
            self._advance(db, now)
            row = db.execute("SELECT * FROM jobs WHERE name=?", (name,)).fetchone()
            if row is None or row["state"] != "pending":
                raise ValueError("only a pending job can be cancelled without a lease")
            db.execute("UPDATE jobs SET state='cancelled' WHERE name=?", (name,))
            self._event(db, name, "cancelled", row["fence"], now)

    def snapshot(self) -> dict[str, Any]:
        with self._connection() as db:
            # One read transaction keeps metadata/jobs/events in the same snapshot.
            db.execute("BEGIN")
            meta = db.execute("SELECT * FROM meta").fetchone()
            if meta is None or meta["version"] != 1:
                raise ValueError("unsupported store schema")
            jobs = [
                dict(row)
                for row in db.execute(
                    "SELECT name,cpus,state,fence,deadline FROM jobs ORDER BY ordinal"
                )
            ]
            events = [
                dict(row) for row in db.execute("SELECT * FROM events ORDER BY seq")
            ]
            return {
                "schema_version": 1,
                "capacity": meta["capacity"],
                "clock": meta["clock"],
                "jobs": jobs,
                "events": events,
            }
