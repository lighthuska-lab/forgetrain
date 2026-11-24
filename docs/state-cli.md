# Durable reservation CLI

The original `python -m forgetrain PLAN --free-cpus N` planner remains unchanged.
The separate `python -m forgetrain.state` interface composes the durable Store API
without executing workers, models or arbitrary commands.

```sh
python -m forgetrain.state create jobs.sqlite examples/cpu-plan.json --capacity 4
python -m forgetrain.state admit jobs.sqlite --now 0 --lease-ms 10000
python -m forgetrain.state status jobs.sqlite --event-limit 100
```

Admission returns a versioned report with a leases array. Each array element is a
strict version-1 single-lease receipt, for example:

```json
{"schema_version":1,"job":"prepare","fence":1,"expires":10000}
```

Save that element (not the surrounding admission report) as receipt.json. A report
can contain multiple receipts; the caller selects the intended job explicitly.

```sh
python -m forgetrain.state renew jobs.sqlite --receipt receipt.json --now 1000 --lease-ms 20000
python -m forgetrain.state finish jobs.sqlite --receipt renewed-receipt.json --now 2000 --outcome succeeded
python -m forgetrain.state cancel jobs.sqlite pending-job --now 3000
```

Renew returns a replacement receipt under `receipt`. Save it before finishing or
renewing again: the previous deadline receipt is stale. Finish accepts succeeded,
failed or cancelled only for the current live receipt; cancel applies only to
pending jobs. Times are explicit nondecreasing integer milliseconds, not inferred
wall-clock times. Exact expiry cannot be renewed; a later admission recovers the
expired reservation with an increased fence. See [store semantics](durable-admission.md).

## Input/output and persistence contract

Each command opens the existing database independently; state survives process
restart. Create validates its complete plan/capacity before exclusive database
creation and never replaces existing data. Other commands never create missing
databases. JSON plans are capped at 1 MiB and receipts at 16 KiB. Only regular JSON
files are accepted; named pipes are refused without a blocking open. Duplicate
keys, nonfinite constants, invalid UTF-8, unknown receipt fields/versions and invalid
receipt types are rejected before mutation. No JSON input is rewritten.

Successful commands emit exactly one JSON object to stdout with schema_version=1
and command, then exit 0. Validation, stale receipt, clock, I/O and SQLite failures
exit 2 with stderr and no partial JSON report. Argument errors also exit 2;
interruption exits 130. An empty admission batch is successful and reports leases=[];
it does not mean that blocked or failed jobs completed.

Status defaults to the last 100 events, with --event-limit in 0–1000. It preserves
chronological sequence order, includes event_count and events_truncated, and does
not advance time or expire work. Snapshot jobs remain bounded by the plan's 1,000
job limit. Reports are capped at 1 MiB. The library's default unbounded snapshot()
behavior remains compatible; bounded snapshots are opt-in via event_limit.

Database commits cannot be atomic with writing to a terminal or redirected stdout.
If the consumer closes output or loses a response after a successful mutation,
inspect status rather than replaying a stale receipt or claiming that no mutation
occurred. Receipts are cooperative fencing metadata, not authentication. Database
paths, contents and local filesystem remain trusted; this is not a sandbox,
production service, signed audit stream or physical CPU enforcement mechanism.

Tests launch real subprocesses for create/admit/renew/finish/cancel/status, inspect
persisted restart state, reject stale receipts and malformed/oversized JSON,
exercise exact expiry and backwards clocks, and preserve the existing planner CLI.
No training performance, model inference or paid infrastructure is involved.
