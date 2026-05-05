from arq.connections import RedisSettings
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.services.orchestrator import process_workflow

async def arq_process_workflow(ctx, record_id: str):
    """
    ARQ worker task wrapper.
    """
    async with AsyncSessionLocal() as session:
        await process_workflow(session, record_id)

async def startup(ctx):
    pass

async def shutdown(ctx):
    pass

class WorkerSettings:
    functions = [arq_process_workflow]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL)
    on_startup = startup
    on_shutdown = shutdown
    max_retries = 5
    retry_delay = 2 # seconds