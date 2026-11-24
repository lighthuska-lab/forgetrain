"""Bounded JSON CLI for durable reservations; never executes training commands."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import stat
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .__main__ import unique_object
from .plan import Plan
from .store import Lease, Store

MAX_PLAN_BYTES = 1024 * 1024
MAX_RECEIPT_BYTES = 16 * 1024
MAX_REPORT_BYTES = 1024 * 1024


def _constant(value: str) -> None:
    raise ValueError("nonfinite JSON constants are not supported")


def _load(path: Path, *, limit: int) -> Any:
    # O_NONBLOCK prevents a named pipe from blocking before the regular-file check.
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("JSON input must be a regular file")
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("JSON input exceeds its byte limit")
    return json.loads(
        raw.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=_constant
    )


def _receipt(path: Path) -> Lease:
    document = _load(path, limit=MAX_RECEIPT_BYTES)
    fields = {"schema_version", "job", "fence", "expires"}
    if not isinstance(document, dict) or set(document) != fields:
        raise ValueError("receipt requires exactly schema_version, job, fence, expires")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ValueError("unsupported receipt schema version")
    lease = Lease(document["job"], document["fence"], document["expires"])
    Store._validate_receipt(lease)
    return lease


def _encoded(lease: Lease) -> dict[str, Any]:
    return {"schema_version": 1, **asdict(lease)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser(
        "create", help="exclusively create a fixed-plan database"
    )
    create.add_argument("database", type=Path)
    create.add_argument("plan", type=Path)
    create.add_argument("--capacity", type=int, required=True)
    for command in ("admit", "renew", "finish", "cancel", "status"):
        child = commands.add_parser(command)
        child.add_argument("database", type=Path)
        if command in {"renew", "finish"}:
            child.add_argument("--receipt", type=Path, required=True)
        if command == "finish":
            child.add_argument(
                "--outcome", choices=("succeeded", "failed", "cancelled"), required=True
            )
        if command == "cancel":
            child.add_argument("job")
        if command in {"admit", "renew", "finish", "cancel"}:
            child.add_argument(
                "--now", type=int, required=True, help="nondecreasing milliseconds"
            )
        if command in {"admit", "renew"}:
            child.add_argument("--lease-ms", type=int, required=True)
        if command == "status":
            child.add_argument("--event-limit", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report: dict[str, Any] = {"schema_version": 1, "command": args.command}
        if args.command == "create":
            plan = Plan.parse(_load(args.plan, limit=MAX_PLAN_BYTES))
            store = Store.create(args.database, plan, capacity=args.capacity)
            report["state"] = store.snapshot(event_limit=100)
        else:
            # Parse a complete receipt before opening or mutating the database.
            receipt = (
                _receipt(args.receipt) if args.command in {"renew", "finish"} else None
            )
            store = Store(args.database)
            if args.command == "admit":
                report["leases"] = [
                    _encoded(lease)
                    for lease in store.admit(now=args.now, lease_ms=args.lease_ms)
                ]
            elif args.command == "renew":
                assert receipt is not None
                report["receipt"] = _encoded(
                    store.renew(receipt, now=args.now, lease_ms=args.lease_ms)
                )
            elif args.command == "finish":
                assert receipt is not None
                store.finish(receipt, outcome=args.outcome, now=args.now)
                report.update(job=receipt.job, outcome=args.outcome)
            elif args.command == "cancel":
                store.cancel_pending(args.job, now=args.now)
                report.update(job=args.job, outcome="cancelled")
            else:
                report["state"] = store.snapshot(event_limit=args.event_limit)
        serialized = json.dumps(report, sort_keys=True, allow_nan=False)
        if len(serialized.encode("utf-8")) > MAX_REPORT_BYTES:
            raise ValueError("state report exceeds 1 MiB")
        print(serialized)
        return 0
    except (OSError, ValueError, TypeError, RecursionError, sqlite3.Error):
        print(
            "State command failed; check input schema, receipt, clock and database.",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print("State command interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
