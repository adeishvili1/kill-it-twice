# AGENTS.md — instructions for coding agents working in this repository

Read `SPEC.md` first. It is the source of truth for *what* to build. This file is about *how* to
work here.

## Layout

```
SPEC.md              what to build, decisions, open questions (revise in its own commit, never silently)
README.md            operator docs, ADRs, capacity notes, gate results, deviations log
Makefile             up / down / seed / verify / logs / test
verify.sh            the gate runner (bash); verify/lib.sh holds helpers
docker-compose.yml   the whole system
db/init/*.sql        schema + triggers, applied by the postgres image on first start (numbered, append-only)
db/seed/seed.sql     data generator; SEED_ROWS is substituted by the Makefile
apps/pipeline        NestJS — source reader, backfill, incremental, sinks, checkpoint, DLQ, metrics, control + sim API
apps/consumer        NestJS — RabbitMQ consumer → consumed_events, /metrics, /health
apps/chaos           tiny Node service with the Docker socket; stop/start containers for the UI
apps/ui              Angular standalone app, served by nginx which proxies /api → pipeline, /chaos → chaos
```

## Conventions

- One NestJS module per concern (`backfill`, `incremental`, `sinks/es`, `sinks/rabbit`, `checkpoint`,
  `dlq`, `metrics`, `control`, `simulation`). No god-service.
- SQL through `pg` directly with parameterised queries. No ORM.
- Logs: pino JSON. Every state transition logs an `event` field with a fixed name (see SPEC §6.5).
  Never log a full batch payload.
- Metrics: prom-client. Metric names are part of the contract with `verify.sh` and the README — do not
  rename them. Add new ones freely.
- Config via environment variables with defaults in `config.ts`; runtime-tunable values (`batch_size`,
  `incremental_interval_ms`) live in `pipeline_state`/memory and are exposed via `/api/control/params`.
- Errors from sinks are classified in one place (`sinks/es/bulk-classifier.ts`). Unit-test that file.
- Keep each app self-contained: its own `package.json`, `Dockerfile`, no cross-app imports.
- Ports: pipeline 3000, consumer 3001, chaos 3002, ui 4200, postgres 5432, es 9200, rabbitmq 5672/15672.

## Do not touch without updating SPEC.md and README first

- The delivery guarantee semantics (checkpoint after both acks; idempotency by `(id, version)`).
- The `verify.sh` report line format.
- The `db/init` numbering (append a new file, never edit an applied one).
- The soft-delete rule (no ES delete operations).

## How to check your own work

```
make up            # build + start everything
make seed          # 1M rows (SEED_ROWS=100000 make seed for a quick loop)
make verify        # all five gates; exit code != 0 on any FAIL
make test          # unit tests in pipeline and consumer
make logs          # tail pipeline + consumer
```

- Before claiming a gate passes, run `make verify` and paste the real output in your summary.
- If a gate fails, do not weaken the assertion; fix the system or record the FAIL honestly in README.
- When you find that SPEC.md does not match what you had to build, stop, revise SPEC.md in a
  separate commit (`docs: SPEC vN — <what changed and why>`), and add an entry to the README
  section "Where AI deviated from the spec".
- Commit small, one concern per commit; pair each feature commit with the verify commit that proves it.
