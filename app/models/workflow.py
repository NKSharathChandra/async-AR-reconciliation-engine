import enum
from datetime import datetime
from sqlalchemy import Column, String, Enum, DateTime, JSON, Integer
from app.core.database import Base

class Stage(enum.IntEnum):
    PENDING = 0
    INGESTION = 1
    MATCHING = 2
    VALIDATION = 3
    ROUTING = 4
    COMPLETED = 5

class Status(enum.Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class WorkflowRecord(Base):
    __tablename__ = "workflows"

    id = Column(String, primary_key=True, index=True) # Idempotency key
    status = Column(Enum(Status), default=Status.PENDING, nullable=False)
    current_stage = Column(Enum(Stage), default=Stage.PENDING, nullable=False)
    payload = Column(JSON, nullable=False)
    result_data = Column(JSON, default={}, nullable=False)
    retries = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)