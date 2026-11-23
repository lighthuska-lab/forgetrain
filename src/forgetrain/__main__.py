"""Validate a JSON plan and print its first CPU admission batch."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from forgetrain.plan import Plan


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("--free-cpus", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        with args.plan.open("rb") as stream:
            raw = stream.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("plan exceeds 1 MiB")
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
        plan = Plan.parse(document)
        states = {job.name: "pending" for job in plan.jobs}
        selected = plan.select(states, free_cpus=args.free_cpus)
        print(json.dumps({"version": 1, "ready": selected, "jobs": len(plan.jobs)}))
        return 0
    except (OSError, ValueError, RecursionError):
        print(
            "Invalid or unreadable plan; check schema and CPU capacity.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
