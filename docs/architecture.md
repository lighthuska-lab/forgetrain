# Decision 0001: begin with a pure planning boundary

Status: local proposal, November 22, 2026. Owner: Light Huska.

The first requirement is rejecting invalid orchestration plans and making
resource admission reproducible without paid infrastructure. A frozen job DAG
and pure snapshot selector provide a small testable boundary. Kubernetes and
distributed execution would introduce operational dependencies before a
single-host lifecycle contract exists; they are deferred.

Plan v1 accepts only version/jobs and name/needs/cpus fields. Unknown execution
fields are rejected. Traversal is iterative; worst-case validation is quadratic
in job count for a chain, bounded at 1,000 jobs. JSON duplicate keys are rejected
at the CLI boundary. The library accepts an already-decoded document.

Next design gate: persistent transitions must atomically reserve capacity and
fence stale workers. The current selector is not a lock or execution guarantee.
An independent reviewer must validate state and admission semantics before that
implementation is merged. Training experiments require separate provenance,
hardware, dependency, configuration and metrics records.
