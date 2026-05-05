import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.endpoints import router as api_router
from app.core.database import engine, Base
import app.models.workflow # Ensure models are registered

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB (in production use alembic, this is for simplicity)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(title="Async AR Reconciliation Engine", lifespan=lifespan)

app.include_router(api_router, prefix="/api")

@app.get("/health")
def health_check():
    return {"status": "ok"}