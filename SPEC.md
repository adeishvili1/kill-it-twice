# SPEC — "Kill It Twice": crash-tolerant replication pipeline

| Rev | Date       | Change |
|-----|------------|--------|
| v1  | 2026-09-14 | Initial specification, written before any code. |
| v2  | 2026-09-14 | After the first `verify.sh` run (200k rows): retry cap 30 s → 5 s (§6.3); DLQ entries auto-resolve on a later successful write (§6.4); ES 8 names the per-item rejection `document_parsing_exception`, not `mapper_parsing_exception` (§6.4); resolved Q1, Q2, Q5 (§10). |

This document is the brief I hand to the coding agent. It states what must be built, the
constraints, the decisions I have already made (and why), and what is deliberately left open.
When implementation diverges from this document, the document is revised in its own commit and
the divergence is recorded in README ("Where AI deviated from the spec").

---

## 1. Goal

Replicate a relational table from a source database into two sinks, continuously, and survive
the five failure scenarios in the assignment (G1–G5) in a way that a script can prove.

```
Postgres (source)  ──►  pipeline  ──►  Elasticsearch  (current state, searchable)
                                  └──►  RabbitMQ      ──► consumer (independent service)
```

Two modes run **at the same time** in the same pipeline process:

- **Backfill**: one-off pass over all existing rows (1,000,000 by default).
- **Incremental**: continuous pass over changes, polled at a configurable interval.

Plus a UI that shows the pipeline state and lets an operator control it and trigger failures.

## 2. Non-goals (explicitly out of scope)

- Multi-tenant / multi-table replication. One table, `products`.
- Schema evolution of the source table.
- Exactly-once *delivery*. We give at-least-once delivery with idempotent sinks (see §5).
- Hard deletes. The source uses soft delete (`deleted_at`); a hard delete is not replicated.
- Production hardening: auth on the UI/API, TLS, secrets management, multi-node ES/RMQ.
- Logical decoding / Debezium as the change source (see ADR in README; trigger table instead).
- Horizontal scaling of the pipeline (single process, single backfill cursor).

## 3. Constraints

- Stack: NestJS (pipeline, consumer), Angular (UI), Postgres 16, Elasticsearch 8, RabbitMQ 3,
  Docker Compose. Node 24. Everything starts with `docker compose up`.
- Host budget: Docker Desktop with 8 GB RAM. ES gets 1 GB heap; total stack must fit in ~5 GB.
- `make seed` must load 1,000,000 rows in well under a minute (SQL `generate_series`, not app code).
- `make verify` must run all five gates unattended in under 15 minutes and exit non-zero on FAIL.
- Batch size 500 (the assignment's number). Configurable at runtime from the UI.
- No shared npm workspace; each app is self-contained with its own Dockerfile.
- Docs in English. Identifiers in English.

## 4. Data model

### 4.1 Source (`db/init/001_schema.sql`)

```sql
products(
  id          bigserial primary key,
  sku         text not null,
  name        text not null,
  category    text not null,
  price       numeric(12,2) not null,
  stock       int not null,
  attributes  jsonb not null default '{}',   -- free-form; ES maps it strictly (G4 vector)
  version     bigint not null default 1,     -- monotonic per row; bumped by trigger on UPDATE
  updated_at  timestamptz not null default clock_timestamp(),
  deleted_at  timestamptz null               -- soft delete only
)

change_log(
  seq         bigserial primary key,         -- incremental cursor
  product_id  bigint not null,
  op          text not null,                 -- 'insert' | 'update' | 'delete'(soft)
  version     bigint not null,
  changed_at  timestamptz not null default clock_timestamp()
)

pipeline_state(key text primary key, status text, last_id bigint, target_max_id bigint,
               cursor_seq bigint, updated_at timestamptz)      -- rows: 'backfill', 'incremental'

dlq(id bigserial, sink text, record_id bigint, version bigint, payload jsonb, error text,
    batch_id text, attempts int, status text, created_at, updated_at,
    unique(sink, record_id))
```

Triggers: `BEFORE UPDATE` bumps `version` and `updated_at`; `AFTER INSERT/UPDATE` appends to
`change_log`. The seed disables the change-log trigger while loading so the incremental loop does
not replay the whole seed; after seeding the incremental cursor is set to `max(seq)`.

### 4.2 Consumer (same Postgres instance, separate schema/table — it is the consumer's own store)

```sql
consumed_events(event_id text primary key,   -- "<product_id>:<version>"
                product_id bigint, op text, version bigint, payload jsonb, consumed_at timestamptz)
```

### 4.3 Elasticsearch index `products`

- `_id` = product id. Write with `version_type=external`, `version` = `products.version`.
- Mapping `dynamic: strict`; `attributes` is an object with `dynamic: strict` and
  `weight_kg: double`. A non-numeric `weight_kg` is rejected per-item with 400 → DLQ (G4).
- `refresh_interval: 30s`, `number_of_replicas: 0` (single node, backfill-friendly).
- Deleted rows are indexed with `deleted_at` set, never removed → `count(products) == count(index)`
  is always a valid assertion.

### 4.4 RabbitMQ

- Exchange `products.events` (topic, durable). Queue `products.consumer` (durable) bound to `#`.
- Message: persistent, `messageId = event_id`, body `{event_id, product_id, op, version, product}`.
- Publisher uses a confirm channel and waits for confirms per batch.

## 5. Delivery guarantee

**At-least-once delivery, effectively-once effect.**

Per batch the pipeline does: read batch → write ES bulk and publish to RMQ (concurrently) →
wait for both acks → commit checkpoint (`pipeline_state`). The checkpoint is the only durable
progress marker. Any crash before the checkpoint commit replays the batch on restart.

Replays are harmless because both sinks are idempotent on `(product_id, version)`:

| Sink        | Mechanism                                   | Evidence metric                          |
|-------------|---------------------------------------------|------------------------------------------|
| ES          | external versioning; stale/replayed → 409   | `duplicates_absorbed_total{sink="es"}`    |
| Consumer    | `consumed_events.event_id` PK, `ON CONFLICT DO NOTHING` | `duplicates_absorbed_total{sink="consumer"}` |

Failure matrix (what happens if the process dies at each point of a batch):

| Crash point                                  | Effect on restart                                  |
|----------------------------------------------|----------------------------------------------------|
| before any sink write                        | batch re-read and written; no side effects existed |
| after ES bulk, before RMQ confirm            | ES writes → 409 (absorbed); RMQ gets the events    |
| after RMQ confirm, before ES bulk completes  | RMQ dupes absorbed by consumer; ES written         |
| after both acks, before checkpoint           | both sinks absorb the full batch                   |
| ES bulk partially applied (node dies mid-bulk)| unapplied items get written, applied ones → 409    |

Ordering: backfill and incremental may both write the same product. Because ES compares
`version`, a backfill write of an older snapshot after an incremental write of a newer one is a
no-op (409). Same-millisecond updates are not a problem because `version` is a counter, not a clock.

## 6. Pipeline behaviour

### 6.1 Backfill
- On `start`: snapshot `target_max_id = max(id)`, `last_id = 0`, `status = running`.
- Loop: `SELECT … WHERE id > last_id AND id <= target_max_id ORDER BY id LIMIT batch_size`,
  write both sinks, checkpoint `last_id = max(id in batch)`. Empty batch → `status = done`.
- On boot: if `status = running` → resume automatically from `last_id`. Set the one-shot gauge
  `backfill_resume_from_id` and log `{"event":"backfill_resumed","from_id":N}`. This is what G1 reads.
- Controls: `start`, `pause`, `resume`, `reset`. `batch_size` is changeable at runtime.

### 6.2 Incremental
- Every `incremental_interval_ms` (default 1000): `SELECT … FROM change_log WHERE seq > cursor
  AND changed_at <= clock_timestamp() - safety_window ORDER BY seq LIMIT batch_size`, join the
  current product row, write both sinks, checkpoint `cursor_seq`.
- `safety_window` (default 2 s) exists because `bigserial` values are assigned at insert time but
  transactions commit out of order; without the window a late-committing row could be skipped.
  This is a known weakness of polling a sequence; the production answer is logical decoding.
- Lag metrics: `incremental_lag_seq = max(seq) - cursor_seq`, `incremental_lag_seconds`
  = age of the oldest unprocessed change (0 when none).

### 6.3 Sink outage (G3)
- ES client: `maxRetries: 0`, `requestTimeout: 10s`. Retrying is owned by the pipeline.
- Whole-request failure or 5xx/429 → wait with capped exponential backoff (500 ms → **5 s**, jitter),
  then retry the same batch. *(v1 said 30 s. The first verify run recovered 19.5 s after ES was
  reachable because the pipeline was asleep inside a 30 s wait. Recovery time is bounded by the cap,
  so the cap is now 5 s; the retry rate at the cap is 0.2/s per waiting loop, still far under the
  1/s "no busy loop" budget.)* While waiting: `sink_up{sink}=0`, pipeline `state=degraded`, both
  loops are parked on the same wait. No checkpoint moves. Nothing is dropped.
- RMQ: connection manager with reconnect backoff; a publish while disconnected waits, it does not spin.
- "No busy loop" is defined as: pipeline CPU < 15 % and `sink_retry_total` growing < 1/s during the outage.

### 6.4 Partial batch failure (G4)
- ES bulk responses are classified **per item**:
  - 2xx → written; 409 → absorbed duplicate;
  - 4xx other than 429 → **DLQ** (`sink='es'`, payload, error reason, batch id, version);
  - 429 / 5xx → the whole batch is retried (this is an ES problem, not a data problem).
- The batch checkpoint still advances when the only failures were DLQ'd items. The batch is never
  rolled back.
- DLQ replay (`POST /api/dlq/replay`, one id or all): re-read the **current** source row and write
  it again. Success or 409 → DLQ row `status = resolved`. Failure → `attempts++`, stays.
- *(v2)* If a record with an open DLQ entry is later written successfully by any path (the row was
  fixed at the source and the incremental loop re-synced it), the entry auto-resolves. Otherwise
  `dlq_size` would over-report and the operator would replay rows that are already correct.
- *(v2)* The rejection ES 8 returns for a wrongly typed field is `document_parsing_exception`
  (status 400). The classifier keys on the status code, not the name, so nothing changed in the
  pipeline; `verify.sh` matches `parsing_exception`.
- RMQ has no per-item failure mode of its own; the consumer keeps its own `consumer_dlq` for
  rows it cannot store.

### 6.5 Observability (G5)
- Prometheus `/metrics` on pipeline and consumer (prom-client). Names are fixed:
  `backfill_position`, `backfill_target`, `backfill_status` (0 idle,1 running,2 paused,3 done),
  `backfill_resume_from_id`, `records_written_total{sink,mode}`, `records_per_second{mode}`,
  `incremental_cursor`, `incremental_lag_seq`, `incremental_lag_seconds`, `dlq_size{sink}`,
  `sink_up{sink}`, `sink_retry_total{sink}`, `duplicates_absorbed_total{sink}`, `pipeline_state`
  (0 healthy, 1 degraded); consumer: `messages_received_total`, `events_stored_total`,
  `duplicates_absorbed_total`, `consumer_dlq_size`.
- `/health` → `{status: ok|degraded, db, sinks:{es,rabbitmq}, backfill, incremental}`.
- Logs: JSON (pino), every significant transition has an `event` field
  (`backfill_started|backfill_resumed|backfill_done|batch_committed|sink_down|sink_up|dlq_item|crash_requested`).
- `/api/status` aggregates everything for the UI; the UI polls it every second.

### 6.6 Simulation hooks (used by verify and the UI)
- `POST /api/sim/crash-after-next-batch` → exit(1) after both sink acks, before checkpoint.
- `POST /api/sim/corrupt {ids}` → `attributes.weight_kg = 'not-a-number'` on those rows.
- `POST /api/sim/generate-changes {count, ops}` → random updates/inserts/soft-deletes.
- Sink outages are real container stops. From the UI they go through a tiny `chaos` service that
  owns the Docker socket; from `verify.sh` they are plain `docker stop/start` on the host.

## 7. Gates — how each is proven by `verify.sh`

| Gate | Action                                                                 | Assertion |
|------|------------------------------------------------------------------------|-----------|
| G1   | backfill to ≥30 %, `docker kill` pipeline, `docker compose start`; again at ≥60 % | `backfill_resume_from_id == pipeline_state.last_id` at kill time, > 0; run completes with `es_count == source_count` |
| G2   | same run + `crash-after-next-batch` at ≈80 % (guaranteed replayed batch) | `es _count == source`; `count(*) == count(distinct event_id) == source` in consumer; `duplicates_absorbed_total > 0` on both sinks |
| G3   | start a change generator; `docker stop elasticsearch` 60 s; sample CPU; `docker start` | CPU avg < 15 %, retry rate < 1/s, 0 lost (every generated version present in ES), recovery time measured from ES yellow → `sink_up=1 && lag==0` |
| G4   | corrupt 3 of 500 rows, touch all 500                                    | 497 at new version in ES, `dlq_size{es}==3`, DLQ rows carry payload+error+batch; fix + replay → 0 in DLQ |
| G5   | scrape `/metrics`, `/health`, UI; pause incremental, generate 1000 changes | every metric name present; `backfill_position == pipeline_state.last_id`; lag ≥ 1000 then drains to 0 after resume; UI 200; `backfill_resumed` log line exists |

Report format (fixed; verify prints exactly this shape):

```
G1 resume after kill ............ PASS (killed at 412,500 / resumed at 412,500, 0 lost)
G2 no duplicates ................ PASS (1,000,000 source / 1,000,000 sink / 0 dupes, 1,500 replays absorbed)
G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)
G4 partial batch failure ........ PASS (497 written, 3 in DLQ, 3 replayed)
G5 observability ................ PASS
```

`verify.sh` always starts from `make seed` (clean state) and prints per-gate wall time.

## 8. UI (Angular, functional, four screens)

1. **Dashboard** — backfill progress (position / target / %), throughput, incremental lag, DLQ
   counts, health per sink, pipeline state. Polls `/api/status`.
2. **Data** — search the ES index (text + category), open a record to see source row vs indexed
   doc vs last consumed event; a "recent events" list from the consumer, refreshed live.
3. **Control** — start/pause/resume/reset backfill; pause/resume incremental; set batch size and
   interval; DLQ table with error/payload and replay (one / all).
4. **Simulation** — stop/start ES and RabbitMQ; corrupt N rows; generate N changes; crash after next batch.

## 9. Decisions already made

| # | Decision | Why |
|---|----------|-----|
| D1 | Change capture via trigger-fed `change_log` table, not logical decoding | Simplest thing that gives a monotonic cursor; no replication slots, no Debezium container. Weakness (out-of-order commits) mitigated by a safety window and documented. |
| D2 | Checkpoint lives in Postgres, committed after both sink acks | Durable, transactional, one place to read for G1. Redis would add a component without adding durability. |
| D3 | Idempotent sinks via monotonic `version` (not `updated_at`) | Clocks collide in the same ms and are not monotonic; a counter is. ES external versioning gives stale-write protection for free. |
| D4 | Per-item DLQ, batch never rolled back | Bulk API already reports per item; retrying the whole batch on data errors would loop forever. |
| D5 | Soft delete only | Avoids ES tombstone GC resurrecting docs on late backfill writes; keeps count assertions exact. |
| D6 | Backfill and incremental in the same process, separate loops and checkpoints | Assignment requires both simultaneously; a shared process shares the sink-down wait. |
| D7 | UI polls, no websockets/SSE | 1 s polling is "real time" enough for an operator screen; far less code. |
| D8 | Consumer stores events in its own Postgres table | Gives a countable, queryable proof for G2 and a realistic downstream. |
| D9 | `restart: "no"` on the pipeline container | Verify must observe the killed state before restarting; production would use a restart policy, resume-on-boot is what actually matters. |
| D10 | 1,000,000 rows default | ≈1 GB of JSON in flight, 2,000 batches, ~5 min backfill: big enough that "load it all in memory" and "restart from zero" are visibly wrong, small enough for a 15-minute verify. |

## 10. Open questions (to be resolved during implementation, recorded as SPEC revisions)

- ~~Q1 Throughput target is unknown until measured.~~ **Resolved v2:** a single sequential loop
  does ~17,000 rows/s on the dev machine (100k rows in 5.8 s); no pipelining needed for 1M rows.
  The first measurement showed 500 rows/s — a bug (bigint ids arrived as strings and broke a
  gauge, every batch fell into the 1 s error sleep), not a capacity limit.
- ~~Q2 Where to compute `records_per_second`.~~ **Resolved v2:** pipeline-side, 10 s sliding window.
- Q3 Whether ES `refresh_interval` should be reset to `1s` after backfill for the Data screen.
- Q4 How the consumer batches acks without losing the "independent service" property.
- ~~Q5 Exact CPU threshold for G3.~~ **Resolved v2:** measured 0.6–1.0 % during a 30 s outage; the
  15 % assertion stays as a generous ceiling.

## 11. Acceptance

`docker compose up -d --build && make seed && make verify` prints five PASS lines and exits 0 on
a clean machine with Docker, make, curl and jq installed. The UI at http://localhost:4200 covers
the four screens. README contains the sections the assignment lists.
