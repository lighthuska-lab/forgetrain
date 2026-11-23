# Trusted local artifact provenance

`ArtifactRegistry` records bounded local file provenance without loading model or
checkpoint formats. It is a separate SQLite database from the scheduler, so no
scheduler migration, worker integration or completion claim is implied.

```python
from forgetrain.artifacts import ArtifactRegistry

registry = ArtifactRegistry.create("artifacts.sqlite", root="training-outputs")
record = registry.register(
    "checkpoint-001",
    relative_path="run-1/checkpoint.safetensors",
    kind="checkpoint",
    format="safetensors",
    producer_revision=FULL_CODE_REVISION,
)
report = registry.verify("checkpoint-001")
```

The root must exist and its supplied final component cannot be a symlink. Registry
creation is exclusive and never overwrites an existing file. Root metadata is
bound to the database. Paths must be canonical relative POSIX strings, with no
absolute paths, backslashes, dot segments, NULs or descendant symlinks. The opened
file must be regular. Registry internals and hardlinks to its database are rejected.

Artifacts are limited to 64 MiB and read in 1 MiB blocks. The SHA-256 and byte size
come from bytes actually read. File descriptor/path identity, size and modification
metadata are compared around hashing to reject ordinary concurrent changes. Files
must nevertheless be frozen: these checks are not race-proof protection against a
hostile local process, nor an atomic transaction spanning filesystem and database.
No artifact is rewritten, copied, deserialized, uploaded or executed.

IDs are lowercase identifiers. Kind is checkpoint, metrics or other. Format is a
nonblank UTF-8 declaration of at most 128 characters. Producer revision is a full
lowercase 40- or 64-digit source revision/content digest, not a moving branch name.
The registry validates its shape, not its existence, authorship or relationship to
the artifact. Format/kind declarations are not checkpoint validation or license
verification. Zero-byte files are allowed and hashed, not presented as valid models.

## Immutability and transactions

Registration stores immutable API metadata: path, kind, format, producer revision,
observed content hash/size and an actual UTC registration timestamp. The same ID
with identical provenance is idempotent and preserves its first timestamp; a
changed field or changed file content conflicts instead of overwriting the record.
Concurrent writers serialize in BEGIN IMMEDIATE. Metadata and the registration
audit row commit together with SQLite FULL synchronous durability or both roll back.
SQLite storage and database access remain trusted; a database administrator can
modify the file directly, so this is not tamper-evident storage or authentication.

`get(id)` returns a detached record. `verify(id)` rehashes the registered path and
reports expected/observed hashes, sizes and matches, without changing registration.
Missing, nonregular or newly symlinked files fail. Content drift returns matches=false.
`checkpoint_validity_verified` is always false. Checkpoint loading, model quality,
cryptographic signing, external artifact fencing, lease linkage, retention,
remote storage, multi-host behavior and schema migration are not implemented.

## Reproduce

After package installation, run `python examples/artifact_demo.py`. It creates an
original synthetic raw-byte fixture and uses this demo script's SHA-256 as producer
revision; no real training or checkpoint validity is claimed. Tests include actual
bounded file reads, missing/mutated inputs, traversal/symlinks/FIFO rejection,
self-registration/hardlinks, duplicate/conflicting IDs, concurrent writers,
injected rollback, restart and a process crash before transaction commit.

Reports contain artifact IDs, relative paths and provenance metadata, which may
be sensitive. Review them before publication. No private model files or training
outputs are bundled with the project. No paid resources or service are required.
