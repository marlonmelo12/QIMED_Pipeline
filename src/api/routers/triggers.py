from datetime import datetime
import uuid
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.metadata.database import get_db
from src.metadata.models import JobStatus, Job, Run, PipelineState, IngestionStrategy
from src.orchestration.service import OrchestrationService
from pydantic import BaseModel

router = APIRouter(prefix="/pipeline", tags=["orchestration"])


class TriggerRequest(BaseModel):
    pipeline_id: Optional[str] = "qimed_master_pipeline_end_to_end"
    connection_id: str
    watermark_timestamp: Optional[str] = None
    watermark: Optional[str] = None
    mode: str = "incremental"


class JobStatusUpdate(BaseModel):
    job_id: Optional[str] = None
    run_id: Optional[str] = None
    dag_id: str
    dag_run_id: str
    status: str
    watermark: Optional[str] = None
    connection_id: Optional[str] = None
    source: Optional[str] = None
    entity: Optional[str] = None
    execution_time_seconds: Optional[float] = 0.0
    rows_read: Optional[int] = 0
    rows_written: Optional[int] = 0
    rows_rejected: Optional[int] = 0
    error_message: Optional[str] = None


@router.post("/trigger", status_code=202)
async def trigger_pipeline(
    req: TriggerRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Recebe comando de execução on-demand com connection_id e watermark_timestamp.
    Dispara execução assíncrona desacoplada do request HTTP.
    """
    effective_wm = req.watermark_timestamp or req.watermark
    orch_service = OrchestrationService(db)
    
    conf = {
        "watermark": effective_wm,
        "watermark_timestamp": effective_wm,
        "mode": req.mode
    }

    try:
        job_id = await orch_service.trigger_pipeline(
            pipeline_id=req.pipeline_id or "qimed_master_pipeline_end_to_end",
            connection_id=req.connection_id,
            conf=conf
        )
        return {
            "status": "Accepted",
            "job_id": job_id,
            "pipeline_id": req.pipeline_id or "qimed_master_pipeline_end_to_end",
            "connection_id": req.connection_id,
            "watermark_timestamp": effective_wm,
            "message": "Execução solicitada com sucesso."
        }
    except Exception as e:
        # Fallback de persistência transacional no Control Plane caso o orquestrador esteja temporariamente offline
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        run_id = f"run-{uuid.uuid4().hex[:8]}"
        job = Job(
            job_id=job_id,
            connection_id=req.connection_id,
            pipeline_id=req.pipeline_id or "qimed_master_pipeline_end_to_end",
            status=JobStatus.QUEUED
        )
        run = Run(
            run_id=run_id,
            job_id=job_id,
            status=JobStatus.QUEUED
        )
        db.add(job)
        db.add(run)
        await db.commit()
        return {
            "status": "Accepted",
            "job_id": job_id,
            "pipeline_id": req.pipeline_id or "qimed_master_pipeline_end_to_end",
            "connection_id": req.connection_id,
            "watermark_timestamp": effective_wm,
            "message": "Execução enfileirada no Control Plane."
        }


@router.post("/status")
async def update_pipeline_status(
    payload: JobStatusUpdate,
    db: AsyncSession = Depends(get_db)
):
    """
    Recebe atualizações de status de execução disparadas pelos callbacks do Airflow.
    Atualiza atomicamente as tabelas jobs, runs e pipeline_state no Control Plane PostgreSQL.
    """
    if payload.job_id:
        job = await db.get(Job, payload.job_id)
        if job:
            try:
                job.status = JobStatus(payload.status)
            except ValueError:
                pass

    if payload.run_id:
        run = await db.get(Run, payload.run_id)
        if run:
            try:
                run.status = JobStatus(payload.status)
            except ValueError:
                pass
            run.error_message = payload.error_message
            run.rows_read = payload.rows_read or 0
            run.rows_written = payload.rows_written or 0
            run.rows_rejected = payload.rows_rejected or 0

    # Avanço Atômico do Watermark no PostgreSQL (Regras 16, 21 e 35)
    if payload.watermark and payload.status in ["SUCCEEDED", "SUCCEEDED_WITH_WARNINGS"]:
        stmt = select(PipelineState).where(PipelineState.pipeline_id == payload.dag_id)
        res = await db.execute(stmt)
        pipe_state = res.scalars().first()

        if pipe_state:
            pipe_state.last_successful_watermark = payload.watermark
            pipe_state.last_successful_run_id = payload.run_id or payload.dag_run_id
            pipe_state.updated_at = datetime.utcnow()
        else:
            new_state = PipelineState(
                pipeline_id=payload.dag_id,
                connection_id=payload.connection_id or "datasus_ftp",
                source=payload.source or "datasus",
                entity=payload.entity or "master_lakehouse",
                strategy=IngestionStrategy.TIMESTAMP,
                last_successful_watermark=payload.watermark,
                last_attempted_watermark=payload.watermark,
                last_successful_run_id=payload.run_id or payload.dag_run_id,
                updated_at=datetime.utcnow()
            )
            db.add(new_state)

    await db.commit()
    return {"status": "recorded", "payload": payload.model_dump()}


@router.get("/state/{pipeline_id}")
async def get_pipeline_state(
    pipeline_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Consulta o watermark atual persistido no Control Plane PostgreSQL."""
    stmt = select(PipelineState).where(PipelineState.pipeline_id == pipeline_id)
    res = await db.execute(stmt)
    state = res.scalars().first()
    if not state:
        raise HTTPException(status_code=404, detail="Pipeline state not found")
    return {
        "pipeline_id": state.pipeline_id,
        "connection_id": state.connection_id,
        "source": state.source,
        "entity": state.entity,
        "strategy": state.strategy,
        "last_successful_watermark": state.last_successful_watermark,
        "last_successful_run_id": state.last_successful_run_id,
        "updated_at": state.updated_at
    }
