# Security

This prototype reads local JSON and never executes job payloads. Treat plan
paths and files as untrusted; use trusted local files. It does not provide a
sandbox or multi-tenant service. Never put secrets in plans or issue reports.
Before public publication, configure a private vulnerability reporting channel;
none exists for this unpublished local repository yet. Future execution support
requires explicit capabilities, isolation and a threat-model review.

The SQLite store assumes trusted exclusive database-file ownership. Lease expiry
releases logical capacity without terminating an external worker; fencing only
protects database updates. See docs/durable-admission.md before connecting workers.
