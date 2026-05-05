import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.core.database import Base, engine, AsyncSessionLocal
from app.models.workflow import Status, Stage, WorkflowRecord
from app.services.orchestrator import process_workflow
from app.services import stages
from app.api.endpoints import get_redis_pool


SAMPLE_AR_PAYLOAD = {
    "Customer ID": "CUST-0001",
    "Customer Name": "Customer 0001",
    "Customer Balance": 100.0,
    "Invoice Total": 1000.0, "Invoice applied amount": 0.0, "Invoice exchange rate": 1.0,
    "Payment Total": 800.0, "Payment applied amount": 0.0, "Payment exchange rate": 1.0,
    "Credit Total": 100.0, "Credit applied amount": 0.0, "Credit exchange rate": 1.0,
    "Adjustment Total": 0.0, "Adjustment applied amount": 0.0, "Adjustment exchange rate": 1.0,
}


class MockRedisPool:
    async def enqueue_job(self, *args, **kwargs):
        pass


async def override_get_redis_pool():
    return MockRedisPool()


app.dependency_overrides[get_redis_pool] = override_get_redis_pool


@pytest_asyncio.fixture(autouse=True)
async def setup_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest.mark.asyncio
async def test_health_check():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_workflow_idempotency_and_state():
    payload = {"idempotency_key": "test-123", "payload": SAMPLE_AR_PAYLOAD}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response1 = await ac.post("/api/workflows", json=payload)
        assert response1.status_code == 200
        assert response1.json()["id"] == "test-123"
        assert response1.json()["status"] == "PENDING"

        response2 = await ac.post("/api/workflows", json=payload)
        assert response2.status_code == 200
        assert response2.json()["id"] == "test-123"


@pytest.mark.asyncio
async def test_bulk_csv_upload_and_dedup():
    csv_body = (
        "Customer ID,Customer Name,Customer Balance,"
        "Invoice Total,Invoice applied amount,Invoice exchange rate,"
        "Payment Total,Payment applied amount,Payment exchange rate,"
        "Credit Total,Credit applied amount,Credit exchange rate,"
        "Adjustment Total,Adjustment applied amount,Adjustment exchange rate\n"
        "CUST-A,Customer A,100,1000,0,1.0,800,0,1.0,100,0,1.0,0,0,1.0\n"
        "CUST-B,Customer B,50,500,0,1.0,400,0,1.0,50,0,1.0,0,0,1.0\n"
    )
    files = {"file": ("erp.csv", csv_body, "text/csv")}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r1 = await ac.post("/api/workflows/bulk", files=files)
        assert r1.status_code == 200
        body1 = r1.json()
        assert body1["total"] == 2 and body1["created"] == 2 and body1["duplicate"] == 0

        # Re-upload — both rows should now be reported as duplicates.
        r2 = await ac.post("/api/workflows/bulk", files=files)
        body2 = r2.json()
        assert body2["created"] == 0 and body2["duplicate"] == 2

        listing = await ac.get("/api/workflows")
        assert listing.status_code == 200
        ids = {row["id"] for row in listing.json()}
        assert {"CUST-A", "CUST-B"} <= ids


@pytest.mark.asyncio
async def test_bulk_upload_rejects_missing_customer_id_column():
    csv_body = "Foo,Bar\n1,2\n"
    files = {"file": ("bad.csv", csv_body, "text/csv")}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/api/workflows/bulk", files=files)
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_retry_endpoint_re_enqueues_failed_workflow():
    async with AsyncSessionLocal() as db:
        rec = WorkflowRecord(id="retry-me", payload=SAMPLE_AR_PAYLOAD, status=Status.FAILED)
        db.add(rec)
        await db.commit()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/api/workflows/retry-me/retry")
        assert r.status_code == 200
        assert r.json()["status"] == "PENDING"

    # Completed workflows are not reset.
    async with AsyncSessionLocal() as db:
        rec = await db.get(WorkflowRecord, "retry-me")
        rec.status = Status.COMPLETED
        await db.commit()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/api/workflows/retry-me/retry")
        assert r.status_code == 200
        assert r.json()["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_orchestrator_resumes_from_failure(monkeypatch):
    # Disable random failures so only the forced one fires.
    monkeypatch.setattr(stages, "FAILURE_RATE", 0.0)

    original_validation = stages.run_validation

    async def mock_fail_validation(data):
        raise stages.SimulatedFailureError("Forced Failure")

    monkeypatch.setattr(stages, "run_validation", mock_fail_validation)

    async with AsyncSessionLocal() as db:
        record = WorkflowRecord(id="test-retry-1", payload=SAMPLE_AR_PAYLOAD)
        db.add(record)
        await db.commit()

    async with AsyncSessionLocal() as db:
        with pytest.raises(stages.SimulatedFailureError):
            await process_workflow(db, "test-retry-1")

    async with AsyncSessionLocal() as db:
        record = await db.get(WorkflowRecord, "test-retry-1")
        assert int(record.current_stage) == int(Stage.MATCHING)
        assert record.status == Status.FAILED
        assert record.retries == 1

    monkeypatch.setattr(stages, "run_validation", original_validation)

    async with AsyncSessionLocal() as db:
        await process_workflow(db, "test-retry-1")
        record = await db.get(WorkflowRecord, "test-retry-1")
        assert int(record.current_stage) == int(Stage.COMPLETED)
        assert record.status == Status.COMPLETED
        assert record.result_data["routed_to"] in {"AUTO_CLEAR", "MANUAL_REVIEW", "ANOMALY_QUEUE"}


@pytest.mark.asyncio
async def test_reconciliation_routes_clean_record_to_auto_clear(monkeypatch):
    monkeypatch.setattr(stages, "FAILURE_RATE", 0.0)
    async with AsyncSessionLocal() as db:
        # Customer Balance equals the ERP formula output exactly.
        clean = dict(SAMPLE_AR_PAYLOAD, **{"Customer Balance": 100.0})
        db.add(WorkflowRecord(id="clean-1", payload=clean))
        await db.commit()

    async with AsyncSessionLocal() as db:
        await process_workflow(db, "clean-1")
        record = await db.get(WorkflowRecord, "clean-1")
        assert record.status == Status.COMPLETED
        assert record.result_data["is_valid"] is True
        assert record.result_data["routed_to"] == "AUTO_CLEAR"


@pytest.mark.asyncio
async def test_reconciliation_routes_corrupted_record_to_manual_review(monkeypatch):
    monkeypatch.setattr(stages, "FAILURE_RATE", 0.0)
    async with AsyncSessionLocal() as db:
        bad = dict(SAMPLE_AR_PAYLOAD, **{"Customer Balance": 999.99})
        db.add(WorkflowRecord(id="bad-1", payload=bad))
        await db.commit()

    async with AsyncSessionLocal() as db:
        await process_workflow(db, "bad-1")
        record = await db.get(WorkflowRecord, "bad-1")
        assert record.status == Status.COMPLETED
        assert record.result_data["is_valid"] is False
        assert record.result_data["routed_to"] in {"MANUAL_REVIEW", "ANOMALY_QUEUE"}
