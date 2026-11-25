# Bounded durable event pages

```sh
python -m forgetrain.state events jobs.sqlite --limit 20
python -m forgetrain.state events jobs.sqlite --after-seq 20 --through-seq 80 --limit 20
```

`Store.event_page(after_seq=0, limit=20, through_seq=None)` returns ordered event
rows after the exclusive cursor and at/below an inclusive prefix watermark.
The first call captures the observed maximum sequence; pass its `through_seq`
on subsequent pages to exclude events appended later. `next_after_seq` is the
last returned sequence, or the supplied cursor when empty. `has_more` means more
rows exist inside this prefix, not that the entire history has been audited.
An empty prefix is0 and remains empty when explicitly reused after later appends.
Sequence gaps are permitted; cursors are sequence values rather than row offsets.
A cursor beyond the prefix yields an empty final page.

Limit is a strict integer1–100; cursor/prefix are strict integers0–2^63-1. Booleans,
negative values and oversized requests fail. A supplied prefix cannot exceed the
maximum visible in that read transaction: a future bound would admit later events
and would not be a stable prefix. The page includes limit/cursor/prefix metadata.
The CLI exits0 on a page,2 for invalid input/I/O, with no partial success stdout.

One explicit SQLite read transaction pins schema metadata, maximum sequence and
page rows together. The primary-key range query reads at most limit+1 event rows,
using the extra row only for continuation. It does not count the entire event log,
load job rows, advance the caller's logical clock, expire leases or change state.
Existing status/snapshot behavior remains available. This API requires no caller
clock and does not perform maintenance as a side effect of inspection.

Host/database and directory ancestors must remain trusted. The prefix is stable
under the supported append-only store APIs, not arbitrary SQL edits or database
replacement/corruption. Separate pages are separate transactions. A page is a view
of declared store events, not cryptographic integrity or worker-execution evidence.
No worker command, checkpoint deserialization, GPU, network or paid resource runs.
