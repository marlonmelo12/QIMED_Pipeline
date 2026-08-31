import enum
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Enum, ForeignKey
from sqlalchemy.orm import relationship
from src.metadata.database import Base

class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    SUCCEEDED_WITH_WARNINGS = "SUCCEEDED_WITH_WARNINGS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class IngestionStrategy(str, enum.Enum):
    FULL_SNAPSHOT = "FULL_SNAPSHOT"
    TIMESTAMP = "TIMESTAMP"
    HIGH_WATERMARK = "HIGH_WATERMARK"
    CDC = "CDC"
    PARTITION = "PARTITION"
    FILE_MANIFEST = "FILE_MANIFEST"

class PipelineState(Base):
    __tablename__ = "pipeline_state"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(String, index=True, nullable=False)
    connection_id = Column(String, index=True, nullable=False)
    source = Column(String, nullable=False)
    entity = Column(String, nullable=False)
    strategy = Column(Enum(IngestionStrategy), nullable=False)
    last_successful_watermark = Column(String, nullable=True)
    last_attempted_watermark = Column(String, nullable=True)
    last_successful_run_id = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UploadMetadata(Base):
    __tablename__ = "upload_metadata"

    upload_id = Column(String, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    content_hash = Column(String, index=True, nullable=True)
    size_bytes = Column(Integer, nullable=False)
    source = Column(String, nullable=False)
    received_at = Column(DateTime, default=datetime.utcnow)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    processing_run_id = Column(String, nullable=True)

class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(String, primary_key=True, index=True)
    connection_id = Column(String, index=True, nullable=False)
    pipeline_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)

    runs = relationship("Run", back_populates="job")

class Run(Base):
    __tablename__ = "runs"

    run_id = Column(String, primary_key=True, index=True)
    job_id = Column(String, ForeignKey("jobs.job_id"), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    rows_read = Column(Integer, default=0)
    rows_written = Column(Integer, default=0)
    rows_rejected = Column(Integer, default=0)
    error_message = Column(String, nullable=True)
    
    job = relationship("Job", back_populates="runs")

