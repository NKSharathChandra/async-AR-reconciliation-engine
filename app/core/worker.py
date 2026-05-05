from arq.connections import RedisSettings
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.services.orchestrator import process_workflow

# ARQ retries a job up to MAX_TRIES times. We expose this to the orchestrator
# so the row's status reflects RETRYING vs terminal FAILED accurately.
MAX_TRIES = 5


async def arq_process_workflow(ctx, record_id: str):
    """ARQ task wrapper. `ctx['job_try']` is 1-indexed."""
    is_final_attempt = ctx.get("job_try", 1) >= MAX_TRIES
    async with AsyncSessionLocal() as session:
        await process_workflow(session, record_id, is_final_attempt=is_final_attempt)


async def startup(ctx):
    pass


async def shutdown(ctx):
    pass


class WorkerSettings:
    functions = [arq_process_workflow]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL)
    on_startup = startup
    on_shutdown = shutdown
    # ARQ's actual attribute is `max_tries` (the previous `max_retries` was
    # silently ignored — ARQ fell back to its default of 5).
    max_tries = MAX_TRIES
    # Per-retry backoff in seconds.
    retry_jobs = True
    job_timeout = 60
