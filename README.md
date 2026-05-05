# Async AR Reconciliation Engine

An asynchronous, fault-tolerant workflow engine for processing Account Receivable (AR) reconciliation datasets. It is built using modern Python frameworks to handle high-concurrency tasks safely.

## Architecture Summary

This engine relies on a robust **State-Machine Pattern** built with **FastAPI**, **SQLAlchemy (asyncio)**, **PostgreSQL**, and **ARQ (Redis)**.

- **Idempotency & Safe Duplicates**: Workflows are assigned unique IDs based on client-provided `idempotency_key`s. If a duplicate payload is submitted, the API recognizes the existing record and safely returns the current state instead of spawning duplicate tasks.
- **Resilience & Resumability**: Every workflow transitions through distinct stages (`INGESTION`, `MATCHING`, `VALIDATION`, `ROUTING`). State updates are atomically persisted to PostgreSQL via SQLAlchemy. If a stage randomly fails or the worker crashes, the ARQ queue automatically retries the task. Because the state is tracked in the DB, the orchestrator seamlessly skips past previously completed stages and resumes exactly where it left off, avoiding redundant work.
- **Asynchronous Execution**: The system leverages Python's `asyncio` end-to-end (from the FastAPI endpoints down to the `asyncpg` database driver and Redis queue), ensuring highly scalable I/O-bound parallel execution.
- **Dockerized Ready**: Included is a production-like `docker-compose.yml` that easily spins up PostgreSQL, Redis, the Web API, and the ARQ Worker side-by-side.

## Directory Structure

```text
async-workflow-engine/
├── app/
│   ├── api/
│   │   └── endpoints.py      # REST endpoints (FastAPI)
│   ├── core/
│   │   ├── config.py         # Pydantic configuration management
│   │   ├── database.py       # Async SQLAlchemy engine/sessions
│   │   └── worker.py         # ARQ queue configuration
│   ├── models/
│   │   └── workflow.py       # SQLAlchemy ORM models & Enums
│   ├── schemas/
│   │   └── workflow.py       # Pydantic validation schemas
│   └── services/
│       ├── orchestrator.py   # Core State-Machine Logic
│       └── stages.py         # Mock workflow processing steps
├── tests/
│   └── test_workflow.py      # Pytest coverage
├── docker-compose.yml        # Multi-container local orchestration
├── Dockerfile                # Standard Python image definition
├── requirements.txt          # Dependencies
└── main.py                   # FastAPI application entry point
```

## Running the Application Locally (Docker)

1. Ensure Docker and Docker Compose are installed.
2. Run the application:
   ```bash
   docker-compose up --build
   ```
3. The API will be available at `http://localhost:8000`. You can visit the interactive API docs at `http://localhost:8000/docs`.

### Running Tests

Tests use an in-memory SQLite database and do not require Docker.
1. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Execute Pytest:
   ```bash
   PYTHONPATH=. pytest tests/ -v
   ```

## Hitting the Endpoints

**1. Submit a New Workflow**
```bash
curl -X POST "http://localhost:8000/api/workflows" \
     -H "Content-Type: application/json" \
     -d '{
           "idempotency_key": "record-456",
           "payload": {"amount": 500.0, "invoice_id": "INV-1020"}
         }'
```

**2. Check Workflow Status**
```bash
curl -X GET "http://localhost:8000/api/workflows/record-456"
```
Wait a few seconds and run the GET request again. You'll observe the `current_stage` progressing and the `status` updating from `PROCESSING` to `COMPLETED`. If a failure occurred, you might see `status` as `FAILED` momentarily until the background worker retries and succeeds.