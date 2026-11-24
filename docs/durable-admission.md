# Transactional CPU admission

`Store.create(path, plan, capacity=...)` creates one trusted local SQLite database
exclusively; an existing file is never overwritten. Capacity is an integer 1–1024.
The plan is validated again before any file is created. Each database owns a fixed
plan/capacity. `Store(path)` reopens it without resetting state.

`admit(now=..., lease_ms=...)` uses BEGIN IMMEDIATE to recover expired leases,
calculate free reserved capacity and admit ready jobs atomically in declaration
order. Separate connections serialize reservations. SQLite FULL synchronous
commits preserve the reservation, fencing counter and audit event together.
Deadlines are integer milliseconds; durations are 1–86,400,000 ms. Caller times
must be nondecreasing across successful mutations. Use a consistent trusted clock;
a forward jump can expire work early. This API does not authenticate timestamps.

Admission returns immutable Lease(job, fence, expires) receipts. `finish(receipt,
outcome=..., now=...)` accepts succeeded/failed/cancelled only for the current live
receipt. Expiry occurs exactly at the deadline, requeues the job and releases its
logical reservation. The next admission increments the fencing counter. Old
receipts cannot finish or cancel a newer attempt. A pending task can be cancelled
through `cancel_pending`; running cancellation requires its current receipt.
Failure and cancellation block descendants. No automatic retry of terminal failures
or maximum-attempt policy is implemented; expired leases may be admitted again.

Every successful state mutation and its audit event is transactional. An exception
rolls back the entire operation, including clock/expiry housekeeping. Read-only
`snapshot()` gives one consistent metadata/job/event snapshot and does not expire
work; a later successful mutation performs recovery. There is no plan editing, multi-host coordination, worker process, GPU or
training execution.

## Important execution boundary

Reservations are logical accounting, not OS CPU enforcement. Expiry does not kill
an old worker. Receipts fence database mutations only: external artifact writes or
still-running work need a future worker/artifact protocol before physical-resource
safety can be claimed. Do not use this store as proof that a timed-out process has
stopped. Database access is trusted; receipts are not authorization credentials.
Use local SQLite storage, not a network filesystem; schema migration is absent.

## Reproduce

After installing the package, run `python examples/durable_demo.py`. It creates a
temporary synthetic plan and advances a logical clock; it never trains a model.
`python -m unittest discover -s tests -v` covers eight simultaneous independent
connections competing for capacity, exact expiry, stale receipt fencing, rollback,
restart state and a real child process exiting mid-transaction. No throughput or
training-performance result is claimed.

## Heartbeats through lease renewal

`renew(receipt, now=..., lease_ms=...)` atomically replaces a live deadline with
`now + lease_ms`, which must strictly exceed the existing deadline. Duration and
clock limits match admission. The attempt fence stays unchanged, but the changed
deadline invalidates the previous receipt for both renewal and completion. Always
replace the worker's receipt with the returned value before its next heartbeat or
terminal update. Lost renewal responses cannot safely be retried with an old
receipt; this API intentionally fails stale attempts rather than guessing.

Renewal at or after exact expiry cannot revive an attempt. Expiry/re-admission
increments its fence as before. Concurrent renewals or a renewal/completion race
using the same receipt have at most one successful mutation. Deadline, logical
clock and the renewed audit event commit together or roll back together. A
renewal preserves the CPU reservation; it does not reserve capacity a second time.
These receipts remain a cooperative database protocol, not worker authentication,
a process liveness guarantee or fencing for external writes.

A store binds its database to the resolved absolute path at construction, including
relative paths passed to `Store.create` and `Store`. Changing the process working
directory later does not redirect reads or mutations to another same-named database.
This is path binding, not protection against replacing a trusted database file or
changing its ancestors; the existing trusted local filesystem assumptions apply.
