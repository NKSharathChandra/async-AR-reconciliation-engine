# Async AR Reconciliation Engine

An asynchronous, fault-tolerant workflow engine that reconciles Account-Receivable
records exported from an ERP. Each record flows through a four-stage pipeline
(ingestion → matching → validation → decision routing); every stage transition is
persisted, every transient failure is retried, and every retry resumes from the
last successful stage rather than restarting.

Built on **FastAPI**, **SQLAlchemy (async)**, **PostgreSQL**, **ARQ (Redis)**.

---

## Table of contents

1. [Architecture](#architecture)
2. [Running locally (no Docker)](#running-locally-no-docker)
3. [Running with Docker Compose](#running-with-docker-compose)
4. [Loading the dataset](#loading-the-dataset)
5. [API reference](#api-reference)
6. [Workflow lifecycle](#workflow-lifecycle)
7. [Reconciliation logic](#reconciliation-logic)
8. [Configuration](#configuration)
9. [Tests](#tests)
10. [Project layout](#project-layout)
11. [Operational notes & gotchas](#operational-notes--gotchas)

---

## Architecture

```
┌──────────────┐    POST /api/workflows[/bulk]    ┌──────────────────┐
│   Client     │ ───────────────────────────────▶ │ FastAPI (uvicorn)│
│  (curl/UI)   │ ◀── JSON status / WorkflowResponse │  app.main:app  │
└──────────────┘                                  └────────┬─────────┘
                                                           │ INSERT (idempotent on PK)
                                                           ▼
                                                  ┌──────────────────┐
                                                  │   PostgreSQL     │
                                                  │  (workflows tbl) │
                                                  └────────▲─────────┘
                                                           │ commit per stage
                                                           │
   ┌─── enqueue_job(arq_process_workflow, id) ─────────────┘
   │                                                       │
   ▼                                                       │
┌──────────┐    BLPOP queue:default     ┌──────────────────┴────────────┐
│  Redis   │ ─────────────────────────▶ │ ARQ worker                    │
│          │                            │ app.core.worker.WorkerSettings│
└──────────┘                            │  └─▶ process_workflow()       │
                                        │       INGESTION → MATCHING    │
                                        │       → VALIDATION → ROUTING  │
                                        └───────────────────────────────┘
```

**Why this shape?**

- **Idempotency at the boundary.** The `idempotency_key` (or `Customer ID` for
  bulk uploads) is the workflow row's primary key, so the database itself
  prevents duplicates. Resubmissions return the existing row without enqueueing
  a second job.
- **State in Postgres, not in the worker.** Each stage commits its result before
  the next stage runs, so a worker crash, a redeploy, or a transient failure
  loses zero progress — the next attempt resumes mid-pipeline.
- **Retries belong to ARQ.** The orchestrator just re-raises on transient
  failure; ARQ is the single source of truth for retry budget and backoff.
- **Async end-to-end.** FastAPI → asyncpg → ARQ → all asyncio. One worker
  process can drive thousands of in-flight reconciliations without blocking.

---

## Running locally (no Docker)

You need Python 3.11+ and a Redis server reachable on `localhost:6379`.
The default DB is SQLite (`./test.db`) — no Postgres required for dev.

```bash
# 1. Set up the venv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Start Redis (any one of the following)
redis-server                   # if installed natively
docker run -p 6379:6379 redis:7

# 3. In one terminal — start the API
PYTHONPATH=. uvicorn app.main:app --reload
# → http://localhost:8000   (interactive docs at /docs)

# 4. In a second terminal — start the ARQ worker
PYTHONPATH=. arq app.core.worker.WorkerSettings
```

The first request to the API auto-creates the SQLite schema via the FastAPI
lifespan hook (no Alembic). Drop `test.db` to reset state.

To point at Postgres instead, export `DATABASE_URL` before starting both
processes (see [Configuration](#configuration)).

---

## Running with Docker Compose

Spins up four containers: Postgres 15, Redis 7, the FastAPI web app, and the
ARQ worker.

```bash
docker-compose up --build
# → API at http://localhost:8000
# → Postgres at localhost:5432  (user/password/workflow_db)
# → Redis at localhost:6379
```

Stop and clean up:

```bash
docker-compose down            # keep volumes
docker-compose down -v         # also wipe Postgres data
```

The compose file mounts the repo into both the `web` and `worker` containers,
so code edits are picked up by uvicorn's `--reload`. The worker does **not**
auto-reload — restart it (`docker-compose restart worker`) after editing
`app/services/stages.py` or the orchestrator.

---

## Loading the dataset

The `docs/erp_export.csv` dataset has 1000 synthetic AR rows (~12% with
intentionally corrupted balances — the kind the pipeline is meant to flag).

**Server-side bulk upload (preferred):**

```bash
curl -X POST "http://localhost:8000/api/workflows/bulk" \
     -F "file=@docs/erp_export.csv"
```

Returns a per-row summary:

```json
{
  "total": 1000,
  "created": 1000,
  "duplicate": 0,
  "errored": 0,
  "items": [
    {"line": 2, "id": "CUST-0001", "status": "created"},
    ...
  ]
}
```

Re-uploading the same CSV is safe — every row reports `duplicate` and nothing
is re-enqueued.

**Client-side loader (for ad-hoc throughput tests):**

```bash
PYTHONPATH=. python scripts/load_dataset.py --concurrency 50
PYTHONPATH=. python scripts/load_dataset.py --limit 100   # smoke test
```

Posts each row to `/api/workflows` concurrently. Same idempotency guarantees.

---

## API reference

Base URL: `http://localhost:8000`. Full OpenAPI / interactive docs at `/docs`.

### `POST /api/workflows`

Submit one record. Idempotent on `idempotency_key`.

```bash
curl -X POST "http://localhost:8000/api/workflows" \
     -H "Content-Type: application/json" \
     -d '{
       "idempotency_key": "CUST-0001",
       "payload": {
         "Customer ID": "CUST-0001",
         "Customer Name": "Customer 0001",
         "Customer Balance": 3486.58,
         "Invoice Total": 3892.38, "Invoice applied amount": 241.57, "Invoice exchange rate": 1.1371,
         "Payment Total": 3316.88, "Payment applied amount": 2375.22, "Payment exchange rate": 0.8873,
         "Credit Total": 910.95,  "Credit applied amount": 676.97,  "Credit exchange rate": 1.0237,
         "Adjustment Total": 1310.49, "Adjustment applied amount": 944.51, "Adjustment exchange rate": 1.1211
       }
     }'
```

Response: `WorkflowResponse` (see [schema](#workflowresponse)).

### `POST /api/workflows/bulk`

Multipart upload of an AR CSV (must match `docs/erp_export.csv` columns).
`Customer ID` becomes each row's idempotency key.

| Form field | Type | Notes                                          |
|------------|------|------------------------------------------------|
| `file`     | file | UTF-8 CSV. Numeric columns are coerced to float. |

`400` if the CSV is missing the `Customer ID` column. Per-row errors (bad
numerics, blank IDs) are reported in the `items[]` array, not as failures.

### `GET /api/workflows`

List workflows, newest first.

| Query param | Type   | Default | Notes                                                |
|-------------|--------|---------|------------------------------------------------------|
| `status`    | enum   | —       | `PENDING` / `PROCESSING` / `RETRYING` / `COMPLETED` / `FAILED` |
| `limit`     | int    | 100     | 1–1000                                               |
| `offset`    | int    | 0       | for paging                                           |

### `GET /api/workflows/{id}`

Single record. `404` if unknown.

### `POST /api/workflows/{id}/retry`

Force-re-enqueue a record whose ARQ retry budget was exhausted (`FAILED`) or
that's stuck (`PROCESSING` / `RETRYING` after a worker crash). Resets `status`
to `PENDING` and pushes a fresh job onto Redis. **`current_stage` is
preserved**, so the orchestrator still resumes mid-pipeline. No-op for
`COMPLETED` rows.

### `WorkflowResponse`

```jsonc
{
  "id": "CUST-0001",
  "status": "COMPLETED",          // PENDING | PROCESSING | RETRYING | COMPLETED | FAILED
  "current_stage": "COMPLETED",   // PENDING | INGESTION | MATCHING | VALIDATION | ROUTING | COMPLETED
  "result_data": {
    "ingested": true,
    "customer_id": "CUST-0001",
    "expected_balance": 3486.58,
    "delta": 0.0,
    "abs_delta": 0.0,
    "is_valid": true,
    "likely_cause": null,
    "routed_to": "AUTO_CLEAR"     // AUTO_CLEAR | MANUAL_REVIEW | ANOMALY_QUEUE
  },
  "created_at": "2026-05-06T12:34:56",
  "updated_at": "2026-05-06T12:34:58"
}
```

---

## Workflow lifecycle

```
            ┌───────────┐  enqueue   ┌────────────┐
  POST ───▶ │  PENDING  │ ─────────▶ │ PROCESSING │
            └───────────┘            └─────┬──────┘
                                           │
                  transient stage failure  │  all stages ok
                  (ARQ try N of 5)         │
                              ┌────────────┴─────────────┐
                              ▼                          ▼
                        ┌───────────┐              ┌────────────┐
                        │ RETRYING  │── ARQ retry─▶│ COMPLETED  │
                        └─────┬─────┘              └────────────┘
                              │ try 5 fails
                              ▼
                        ┌───────────┐    POST /retry    ┌──────────┐
                        │  FAILED   │ ────────────────▶ │ PENDING  │
                        └───────────┘                   └──────────┘
```

`RETRYING` exists specifically so a client polling between ARQ attempts doesn't
see a misleading `FAILED` while a successful retry is already on the way. Only
the final attempt's failure escalates to `FAILED`.

---

## Reconciliation logic

Each stage in `app/services/stages.py` does real work against the AR columns
(this mirrors `docs/reconcile.py`):

| Stage         | Behavior                                                                                                                                      |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `INGESTION`   | Validate the payload has the required AR columns. Missing columns → `ValueError` (permanent — no retry).                                       |
| `MATCHING`    | Recompute `expected_balance = (Inv−InvApplied)·rate − (Pay−PayApplied)·rate − (Cred−CredApplied)·rate + (Adj−AdjApplied)·rate`; emit `delta`, `abs_delta`. |
| `VALIDATION`  | `is_valid = abs_delta ≤ 0.05`. On mismatch attach a `likely_cause` (`"X applied exceeds total"` or `"Check {component}/exchange rate/rounding"`).         |
| `ROUTING`     | `AUTO_CLEAR` if valid, `ANOMALY_QUEUE` if cause is an "exceeds total" violation, else `MANUAL_REVIEW`.                                          |

Every stage also rolls a 20% `SimulatedFailureError` to exercise the
retry/resume path — required by the assignment spec. Disable it in tests via
`monkeypatch.setattr(stages, "FAILURE_RATE", 0.0)`.

---

## Configuration

All settings come from environment variables (see `app/core/config.py`):

| Var            | Default                                | Notes                                         |
|----------------|----------------------------------------|-----------------------------------------------|
| `DATABASE_URL` | `sqlite+aiosqlite:///./test.db`        | Use `postgresql+asyncpg://user:pass@host/db` for Postgres. |
| `REDIS_URL`    | `redis://localhost:6379`               | ARQ broker.                                   |

ARQ-specific knobs live on `WorkerSettings` in `app/core/worker.py`:

| Attr           | Value | Notes                                                                                  |
|----------------|-------|----------------------------------------------------------------------------------------|
| `max_tries`    | 5     | Total attempts per job. **Note:** the ARQ attribute is `max_tries`, not `max_retries`. |
| `retry_jobs`   | True  | Re-queue on exception (the default).                                                   |
| `job_timeout`  | 60s   | Per-attempt timeout.                                                                   |

---

## Tests

In-memory SQLite, no Docker, no Redis required (the tests inject a mock pool).

```bash
PYTHONPATH=. pytest tests/ -v

# single test
PYTHONPATH=. pytest tests/test_workflow.py::test_orchestrator_resumes_from_failure -v
```

Coverage: idempotency, bulk upload + dedup, CSV-shape validation, retry
endpoint, mid-pipeline resume after `RETRYING`, terminal `FAILED` after the
last attempt, and the routing decisions for clean vs corrupted records.

---

## Project layout

```
async-workflow-engine/
├── app/
│   ├── api/endpoints.py        # REST endpoints
│   ├── core/
│   │   ├── config.py           # env-var-driven settings
│   │   ├── database.py         # async engine + session factory
│   │   └── worker.py           # ARQ WorkerSettings
│   ├── models/workflow.py      # SQLAlchemy ORM (Stage IntEnum, Status)
│   ├── schemas/workflow.py     # Pydantic request/response models
│   ├── services/
│   │   ├── orchestrator.py     # state-machine driver, resume logic
│   │   └── stages.py           # 4 reconciliation stages + failure injector
│   └── main.py                 # FastAPI entry point (app.main:app)
├── docs/
│   ├── erp_export.csv          # 1000-row synthetic AR dataset
│   ├── reconcile.py            # offline rule-based reconciler (reference)
│   ├── anomalies.py            # offline ML anomaly script (reference, unused by engine)
│   └── generate_ar.py          # script that produced erp_export.csv
├── scripts/load_dataset.py     # ad-hoc client-side bulk loader
├── tests/test_workflow.py      # pytest suite
├── docker-compose.yml          # Postgres + Redis + web + worker
├── Dockerfile
└── requirements.txt
```

> The repo also contains a top-level `main.py` — that's leftover PyCharm
> boilerplate, **not** the FastAPI entry point. The real entry point is
> `app.main:app`.

---

## Operational notes & gotchas

- **Add new stages by editing `pipeline` in `orchestrator.py` *and* adding the
  enum member to `Stage`** (in `app/models/workflow.py`). The IntEnum ordering
  is load-bearing: the orchestrator does `<` comparisons on it to decide which
  stages to skip on resume. Inserting a stage in the middle without renumbering
  will silently break resume for in-flight rows.
- **Don't mutate `result_data` in place.** SQLAlchemy's default `JSON` column
  doesn't dirty-track in-place dict mutations. The orchestrator deliberately
  reassigns `workflow.result_data = dict(current_data)` on every commit so
  changes actually persist. Same applies to anything else you put on a JSON
  column.
- **`ValueError` from a stage is permanent.** Only `SimulatedFailureError` is
  treated as transient; everything else lands in `FAILED` immediately and ARQ
  won't retry. This is intentional — bad input shouldn't waste 5 retry slots.
- **Schema bootstrap is `Base.metadata.create_all`, not Alembic.** Fine for
  this assignment, not for production. If you change the model, drop
  `test.db` (local) or the `workflows` table (Docker Postgres) before
  restarting.
- **The `worker` container does not hot-reload.** Restart it after changing
  any code under `app/services/`.
