"""A synthetic, temporary CPU reservation lifecycle; never executes a job."""

import json
import tempfile
from pathlib import Path

from forgetrain.plan import Plan
from forgetrain.store import Store

with tempfile.TemporaryDirectory() as directory:
    plan = Plan.parse(
        {
            "version": 1,
            "jobs": [
                {"name": "prepare", "needs": [], "cpus": 1},
                {"name": "train", "needs": ["prepare"], "cpus": 2},
            ],
        }
    )
    path = Path(directory) / "jobs.sqlite"
    store = Store.create(path, plan, capacity=2)
    (receipt,) = store.admit(now=0, lease_ms=100)
    Store(path).finish(receipt, outcome="succeeded", now=1)
    store.admit(now=2, lease_ms=100)
    print(json.dumps(store.snapshot(), indent=2))
