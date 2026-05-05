import csv
import io
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from arq import create_pool
from arq.connections import RedisSettings

from app.core.config import settings
from app.core.database import get_db
from app.models.workflow import WorkflowRecord, Status, Stage
from app.schemas.workflow import (
    WorkflowRequest,
    WorkflowResponse,
    BulkSubmitResponse,
    BulkItemResult,
)
from app.services.stages import REQUIRED_COLUMNS

router = APIRouter()

# Numeric columns must be coerced from CSV strings.
_NUMERIC_COLUMNS = {c for c in REQUIRED_COLUMNS if c != "Customer ID"} | {
    "Invoice Total", "Invoice applied amount", "Invoice exchange rate",
    "Payment Total", "Payment applied amount", "Payment exchange rate",
    "Credit Total", "Credit applied amount", "Credit exchange rate",
    "Adjustment Total", "Adjustment applied amount", "Adjustment exchange rate",
    "Customer Balance",
}


async def get_redis_pool():
    return await create_pool(RedisSettings.from_dsn(settings.REDIS_URL))


async def _enqueue_or_get(
    db: AsyncSession, redis_pool, idempotency_key: str, payload: dict
) -> tuple[WorkflowRecord, bool]:
    """Insert + enqueue if new; otherwise return the existing record. Returns (record, created)."""
    existing = await db.get(WorkflowRecord, idempotency_key)
    if existing:
        return existing, False

    record = WorkflowRecord(id=idempotency_key, payload=payload)
    db.add(record)
    await db.commit()
    await db.refresh(record)
    await redis_pool.enqueue_job("arq_process_workflow", record.id)
    return record, True


@router.post("/workflows", response_model=WorkflowResponse)
async def submit_workflow(
    request: WorkflowRequest,
    db: AsyncSession = Depends(get_db),
    redis_pool=Depends(get_redis_pool),
):
    """Submit a single record. Safe for duplicates (idempotent on `idempotency_key`)."""
    record, _ = await _enqueue_or_get(db, redis_pool, request.idempotency_key, request.payload)
    return record


@router.post("/workflows/bulk", response_model=BulkSubmitResponse)
async def bulk_upload_csv(
    file: UploadFile = File(..., description="CSV matching docs/erp_export.csv schema"),
    db: AsyncSession = Depends(get_db),
    redis_pool=Depends(get_redis_pool),
):
    """Upload an AR dataset CSV. Each row becomes one workflow keyed by `Customer ID`.

    Re-uploading the same file is safe: rows whose `Customer ID` already exists
    are reported as `duplicate` and not re-enqueued.
    """
    raw = await file.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "Customer ID" not in reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV missing required 'Customer ID' column")

    items: List[BulkItemResult] = []
    created = duplicate = errored = 0

    for line_no, row in enumerate(reader, start=2):  # line 1 is header
        cid = row.get("Customer ID")
        if not cid:
            errored += 1
            items.append(BulkItemResult(line=line_no, id=None, status="error",
                                        detail="missing Customer ID"))
            continue
        try:
            payload = {k: (float(v) if k in _NUMERIC_COLUMNS and v != "" else v)
                       for k, v in row.items()}
        except ValueError as e:
            errored += 1
            items.append(BulkItemResult(line=line_no, id=cid, status="error",
                                        detail=f"numeric coercion failed: {e}"))
            continue

        _, was_created = await _enqueue_or_get(db, redis_pool, cid, payload)
        if was_created:
            created += 1
            items.append(BulkItemResult(line=line_no, id=cid, status="created"))
        else:
            duplicate += 1
            items.append(BulkItemResult(line=line_no, id=cid, status="duplicate"))

    return BulkSubmitResponse(
        total=len(items), created=created, duplicate=duplicate, errored=errored, items=items,
    )


@router.get("/workflows", response_model=List[WorkflowResponse])
async def list_workflows(
    status: Optional[Status] = Query(None, description="Filter by lifecycle status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List workflows, optionally filtered by status. Newest first."""
    stmt = select(WorkflowRecord).order_by(WorkflowRecord.created_at.desc())
    if status is not None:
        stmt = stmt.where(WorkflowRecord.status == status)
    stmt = stmt.limit(limit).offset(offset)
    result = await db.execute(stmt)
    return list(result.scalars().all())


@router.get("/workflows/{record_id}", response_model=WorkflowResponse)
async def get_workflow_status(record_id: str, db: AsyncSession = Depends(get_db)):
    record = await db.get(WorkflowRecord, record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return record


@router.post("/workflows/{record_id}/retry", response_model=WorkflowResponse)
async def retry_workflow(
    record_id: str,
    db: AsyncSession = Depends(get_db),
    redis_pool=Depends(get_redis_pool),
):
    """Manually re-enqueue a workflow. Useful for FAILED records past ARQ's retry budget,
    or for PROCESSING records orphaned by a worker crash. Idempotent — completed
    workflows return immediately without re-enqueueing."""
    record = await db.get(WorkflowRecord, record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Workflow not found")
    if record.status == Status.COMPLETED:
        return record

    record.status = Status.PENDING
    await db.commit()
    await db.refresh(record)
    await redis_pool.enqueue_job("arq_process_workflow", record.id)
    return record
