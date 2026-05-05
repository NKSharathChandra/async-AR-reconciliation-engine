import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.workflow import WorkflowRecord, Stage, Status
from app.services import stages

logger = logging.getLogger(__name__)

async def process_workflow(db_session: AsyncSession, record_id: str):
    """
    Core orchestrator logic. Resumes from the last successful stage.
    """
    workflow = await db_session.get(WorkflowRecord, record_id)
    if not workflow:
        logger.error(f"Workflow {record_id} not found.")
        return

    if workflow.status == Status.COMPLETED:
        logger.info(f"Workflow {record_id} is already completed.")
        return

    workflow.status = Status.PROCESSING
    await db_session.commit()

    pipeline = [
        (Stage.INGESTION, stages.run_ingestion),
        (Stage.MATCHING, stages.run_matching),
        (Stage.VALIDATION, stages.run_validation),
        (Stage.ROUTING, stages.run_decision_routing)
    ]

    try:
        current_data = dict(workflow.payload)
        if workflow.result_data:
            current_data.update(workflow.result_data)

        for stage_enum, stage_func in pipeline:
            if int(workflow.current_stage) < int(stage_enum):
                logger.info(f"Executing {stage_enum.name} for record {record_id}")
                stage_result = await stage_func(current_data)
                current_data.update(stage_result)
                workflow.current_stage = stage_enum
                # Assign a fresh dict so SQLAlchemy detects the JSON change.
                workflow.result_data = dict(current_data)
                await db_session.commit()
                logger.info(f"Stage {stage_enum.name} succeeded for record {record_id}")
            else:
                logger.info(f"Skipping {stage_enum.name} for record {record_id} (already completed)")

        workflow.status = Status.COMPLETED
        workflow.current_stage = Stage.COMPLETED
        await db_session.commit()
        logger.info(f"Workflow {record_id} fully completed.")

    except stages.SimulatedFailureError as e:
        logger.warning(f"Workflow {record_id} failed at a stage: {e}. Will retry.")
        workflow.status = Status.FAILED
        workflow.retries += 1
        await db_session.commit()
        raise e # Re-raise to let the worker queue (ARQ) handle the retry

    except Exception as e:
        logger.error(f"Workflow {record_id} encountered an unexpected error: {e}")
        workflow.status = Status.FAILED
        await db_session.commit()
        raise e