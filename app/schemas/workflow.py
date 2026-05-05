from pydantic import BaseModel, Field
from typing import Dict, Any, List, Literal, Optional
from datetime import datetime
from app.models.workflow import Status, Stage

class WorkflowRequest(BaseModel):
    idempotency_key: str = Field(..., description="Unique key to prevent duplicate submissions")
    payload: Dict[str, Any] = Field(..., description="The AR reconciliation data to process")

class WorkflowResponse(BaseModel):
    id: str
    status: Status
    current_stage: Stage
    result_data: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BulkItemResult(BaseModel):
    line: int
    id: Optional[str]
    status: Literal["created", "duplicate", "error"]
    detail: Optional[str] = None


class BulkSubmitResponse(BaseModel):
    total: int
    created: int
    duplicate: int
    errored: int
    items: List[BulkItemResult]