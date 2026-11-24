"""Bounded local artifact registry CLI; no checkpoint loading or execution."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from .artifacts import ArtifactRegistry
from .state import _load

MAX_METADATA_BYTES = 16 * 1024
MAX_REPORT_BYTES = 1024 * 1024


def _metadata(path: Path) -> dict[str, Any]:
    row = _load(path, limit=MAX_METADATA_BYTES)
    fields = {
        "schema_version",
        "id",
        "relative_path",
        "kind",
        "format",
        "producer_revision",
    }
    if not isinstance(row, dict) or set(row) != fields:
        raise ValueError("metadata requires exactly the versioned artifact fields")
    if type(row["schema_version"]) is not int or row["schema_version"] != 1:
        raise ValueError("unsupported artifact metadata version")
    return row


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("create", "register", "verify", "list", "audit"):
        child = commands.add_parser(name)
        child.add_argument("database", type=Path)
        if name == "create":
            child.add_argument("root", type=Path)
        elif name == "register":
            child.add_argument("metadata", type=Path)
        elif name == "verify":
            child.add_argument("artifact_id")
        else:
            child.add_argument(
                "--limit", type=int, default=5 if name == "audit" else 20
            )
            child.add_argument("--after")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report: dict[str, Any] = {"schema_version": 1, "command": args.command}
        exit_code = 0
        if args.command == "create":
            ArtifactRegistry.create(args.database, root=args.root)
            report["created"] = True
        else:
            metadata = _metadata(args.metadata) if args.command == "register" else None
            registry = ArtifactRegistry(args.database)
            if args.command == "register":
                assert metadata is not None
                report["registration"] = registry.register(
                    metadata["id"],
                    relative_path=metadata["relative_path"],
                    kind=metadata["kind"],
                    format=metadata["format"],
                    producer_revision=metadata["producer_revision"],
                )
            elif args.command == "verify":
                result = registry.verify(args.artifact_id)
                report["verification"] = result
                exit_code = 0 if result["matches"] else 1
            elif args.command == "audit":
                audit = registry.audit_page(limit=args.limit, after=args.after)
                report["audit"] = audit
                exit_code = (
                    0
                    if all(row["status"] == "matches" for row in audit["results"])
                    else 1
                )
            else:
                report["page"] = registry.list_page(limit=args.limit, after=args.after)
        serialized = json.dumps(report, sort_keys=True, allow_nan=False)
        if len(serialized.encode("utf-8")) > MAX_REPORT_BYTES:
            raise ValueError("report exceeds 1 MiB; request a smaller page")
        print(serialized)
        return exit_code
    except (OSError, ValueError, TypeError, KeyError, RecursionError, sqlite3.Error):
        print(
            "Artifact command failed; check metadata, bounds and local files.",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print("Artifact command interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
