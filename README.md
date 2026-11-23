# ForgeTrain

Local foundation for inspectable training orchestration. The implemented slice
validates a versioned job DAG and deterministically selects a CPU admission batch.
It prevents malformed plans, cycles, unknown dependencies and premature starts
from entering a future executor. This is an early prototype, not a distributed
training platform.

## Run

Python 3.12 or newer; no runtime dependencies or model downloads.

```sh
python -m pip install -e .
python -m forgetrain examples/cpu-plan.json --free-cpus 4
python -m unittest discover -s tests
```

The example returns `prepare` as ready; `train` and `evaluate` require successful
dependencies. Library callers use `Plan.parse(document)` and
`plan.select(states, free_cpus=4)` for subsequent snapshots. States must cover
exactly the job names. Running and succeeded jobs require succeeded dependencies.
Failed and cancelled jobs block descendants. CPU capacity means capacity already
free of running jobs, not total machine capacity.

## Architecture and limitations

`plan.py` is pure schema validation and scheduling policy. The CLI reads a JSON
file and emits the first admission batch. It never starts commands. Declaration
order determines packing: an oversized job is skipped so smaller jobs can fit.
This policy offers no starvation guarantee or global optimal packing. Selection
reserves nothing and does not make concurrent admissions safe. Plans are capped
at 1,000 jobs; CLI files at 1 MiB. No persistence, retries, checkpoint management,
GPU discovery, training, API server or production deployment is implemented.

## Roadmap

1. Review the plan contract and capacity semantics.
2. Add a durable transactional state store and crash-recovery tests.
3. Add an allowlisted CPU worker with explicit cancellation and artifact handling.
4. Establish reproducible local training baselines before GPU/distributed work.

See [architecture](docs/architecture.md), [contributing](CONTRIBUTING.md), and
[security](SECURITY.md). No training performance or model-quality results exist.
