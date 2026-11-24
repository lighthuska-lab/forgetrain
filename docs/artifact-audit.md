# Bounded artifact audit

```sh
python -m forgetrain.artifact_cli audit artifacts.sqlite --limit 5
python -m forgetrain.artifact_cli audit artifacts.sqlite --limit 5 --after previous-id
```

Audits one exclusive-ID keyset page from the immutable registry. Limits are 1–10
artifacts (default 5). Existing per-file hashing permits at most 64 MiB each, so a
page reads at most 640 MiB. No checkpoint is deserialized or executed. No content
bytes, paths or exception messages are exported; reports contain IDs, expected
hash/size, successful observed hash/size and exact outcome counts.

Outcomes: `matches` means observed bytes match registered digest and size;
`drifted` means a completed observation differs; `missing` means the path could not
be found; `unreadable` covers other operating-system errors; `rejected` means an
unsafe, nonregular, oversized or changing input failed existing validation. An
observation failure is never reported as ordinary digest drift, and later records
are still checked. Root loss may make all selected entries missing/unreadable.
Registry/schema errors fail the command instead of producing partial success.

Exit 0 means every selected entry matched (also true for an empty page); exit 1
means any selected entry failed or drifted; exit 2 means invalid arguments or an
unusable registry. Counts cover selected entries only. Always inspect `has_more`
and `next_after`; `complete_registry_audit=false` even for the last/empty page,
because no global snapshot or prior page history is established. `matches` proves
neither checkpoint validity nor scientific quality. Trusted root/database and
stable local filesystem assumptions remain; separate files/pages are not observed
atomically. New IDs may enter earlier pages between requests. No writes to
registered artifacts or registration metadata are performed, and no GPU, network,
optional model runtime or paid resource is used.
