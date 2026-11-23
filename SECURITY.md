# Security

This prototype reads local JSON and never executes job payloads. Treat plan
paths and files as untrusted; use trusted local files. It does not provide a
sandbox or multi-tenant service. Never put secrets in plans or issue reports.
Before public publication, configure a private vulnerability reporting channel;
none exists for this unpublished local repository yet. Future execution support
requires explicit capabilities, isolation and a threat-model review.
