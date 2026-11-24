# Bounded artifact registry CLI

The separate `python -m forgetrain.artifact_cli` interface exposes the existing
local artifact registry without loading, executing or deserializing checkpoint
contents. The existing plan and durable-state CLIs are unchanged.

```sh
python -m forgetrain.artifact_cli create artifacts.sqlite training-outputs
python -m forgetrain.artifact_cli register artifacts.sqlite metadata.json
python -m forgetrain.artifact_cli verify artifacts.sqlite checkpoint-001
python -m forgetrain.artifact_cli list artifacts.sqlite --limit 20
```

The artifact root must already exist. Registry creation is exclusive; it never
replaces an existing file. A registration input is a strict UTF-8 JSON object:

```json
{"schema_version":1,"id":"checkpoint-001","relative_path":"run/checkpoint.bin","kind":"checkpoint","format":"safetensors","producer_revision":"0123456789abcdef0123456789abcdef01234567"}
```

The sample revision and format are declarations, not proof that this file is a
valid checkpoint or was produced by the named revision. Every field is required;
unknown fields, duplicate keys, nonfinite JSON constants, unsupported versions,
invalid UTF-8 and invalid metadata fail. Metadata files must be regular and no
larger than 16 KiB; named pipes are rejected without blocking. Callers cannot
supply a trusted content hash: registration hashes the actual file under the fixed
root, using the registry's 64 MiB cap, canonical relative paths, symlink/escape/
nonregular checks and immutable transaction rules. See [registry contract](artifact-registry.md).

Successful identical registrations are idempotent and preserve the original
registration time. A changed field or changed file conflicts instead of
replacing its ID. Verification re-observes current bytes without changing the
registration. Missing or unsupported files fail; byte drift is a finding rather
than a new registration. No input metadata or artifact contents are rewritten.

## Bounded listing

List reads at most limit+1 records using an indexed, exclusive artifact-ID cursor.
The default page limit is 20; supported limits are 1–100. IDs are sorted
lexicographically, not by registration time. A page contains records, has_more,
next_after and content_files_verified=false. When has_more is true, pass that
next_after value explicitly to the next call:

```sh
python -m forgetrain.artifact_cli list artifacts.sqlite --limit 20 --after checkpoint-001
```

No unbounded dump or automatic pagination is performed. An empty page has
has_more=false and next_after=null. Pages are read-only snapshots individually;
registrations added between pages can affect a scan, so pagination is not one
whole-registry atomic snapshot. Listing shows recorded provenance only; it does
not open artifact files or attest that stored hashes still match current bytes.

## Output, errors and trust

Successful commands emit exactly one JSON object with schema_version=1 and
command. Exit 0 means command success or a matching verification; exit 1 is a
complete verification report with matches=false; exit 2 is invalid input, conflict,
missing/corrupt registry, path/I/O or SQLite failure with stderr and no partial
stdout. Argument errors exit 2; interruption exits 130. Reports are capped at
1 MiB; if a large listing page exceeds that limit, request a smaller page.

Database commit and stdout delivery are not atomic. A lost registration response
can be resolved by identical idempotent registration or reading the recorded page,
provided the artifact remains frozen. Records include IDs, relative paths and
provenance labels that may be sensitive; review reports before sharing them.
Trusted local database/filesystem assumptions remain: metadata is not checkpoint
validity, authentication, external-write fencing or hostile-race isolation.
No checkpoint is loaded and no worker, model, deployment or paid resource runs.

Tests use independent subprocesses to verify persistence/restart, idempotence,
conflicts, byte drift, unsafe paths, malformed/oversized JSON, exclusive creation,
missing/corrupt databases and bounded sorted pagination without mutation.
