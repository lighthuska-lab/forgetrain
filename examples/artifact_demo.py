"""Register and verify a disposable synthetic artifact; no training/model loading."""

import hashlib
import json
import tempfile
from pathlib import Path

from forgetrain.artifacts import ArtifactRegistry

with tempfile.TemporaryDirectory() as directory:
    base = Path(directory)
    root = base / "outputs"
    root.mkdir()
    (root / "fixture.bin").write_bytes(b"synthetic fixture, not a trained checkpoint\n")
    registry = ArtifactRegistry.create(base / "registry.sqlite", root=root)
    # This fixture's producer is the exact bytes of this demonstration script.
    producer = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    record = registry.register(
        "fixture-1",
        relative_path="fixture.bin",
        kind="other",
        format="synthetic-raw-bytes",
        producer_revision=producer,
    )
    reopened = ArtifactRegistry(base / "registry.sqlite")
    print(
        json.dumps(
            {
                "synthetic": True,
                "registration": record,
                "verification": reopened.verify("fixture-1"),
            },
            indent=2,
        )
    )
