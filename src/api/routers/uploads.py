import os
from fastapi import APIRouter, File, UploadFile, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.metadata.database import get_db
from src.metadata.models import UploadMetadata, JobStatus
from src.ingestion.landing_zone import LandingZoneManager
from src.orchestration.service import OrchestrationService
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

landing_zone_mgr = LandingZoneManager()


@router.post("/upload", status_code=202)
async def upload_file(
    file: UploadFile = File(...),
    source_format: str = "generic_csv",
    db: AsyncSession = Depends(get_db)
):
    """
    Recebe arquivo via streaming (SpooledTemporaryFile) com validação de idempotência no PostgreSQL.
    Grava na Landing Zone temporária e delega o processamento pesado exclusivamente ao Airflow
    através do OrchestrationService (Regra 21: Zero Heavy Tasks inside FastAPI process).
    """
    upload_meta = await landing_zone_mgr.save_upload(
        file_data=file.file,
        filename=file.filename,
        db=db
    )
    file_path = os.path.join(landing_zone_mgr.landing_dir, f"{upload_meta.upload_id}_{file.filename}")

    if upload_meta.processing_run_id and upload_meta.status in [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.SUCCEEDED]:
        return {
            "status": "Accepted",
            "upload_id": upload_meta.upload_id,
            "job_id": upload_meta.processing_run_id,
            "message": "Arquivo idêntico já recebido anteriormente (idempotência confirmada)."
        }

    orch_service = OrchestrationService(db)
    conf = {
        "upload_id": upload_meta.upload_id,
        "file_path": file_path,
        "original_filename": file.filename,
        "content_hash": upload_meta.content_hash,
        "source_format": source_format
    }

    try:
        job_id = await orch_service.trigger_pipeline(
            pipeline_id="process_upload_dag",
            connection_id="local_upload",
            conf=conf
        )
        upload_meta.processing_run_id = job_id
        upload_meta.status = JobStatus.QUEUED
        await db.commit()

        logger.info(
            f"[CONTROL PLANE UPLOAD] Upload {upload_meta.upload_id} delegado ao Airflow com job_id={job_id}"
        )

        return {
            "status": "Accepted",
            "upload_id": upload_meta.upload_id,
            "job_id": job_id,
            "message": "Upload recebido e delegado com sucesso ao orquestrador."
        }
    except Exception as e:
        logger.error(f"[CONTROL PLANE UPLOAD] Falha ao disparar pipeline no orquestrador: {e}")
        upload_meta.status = JobStatus.FAILED
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Erro ao delegar processamento ao orquestrador: {str(e)}")


@router.get("/upload/{upload_id}/status")
async def get_upload_status(upload_id: str, db: AsyncSession = Depends(get_db)):
    """Consulta o status de processamento do upload no Control Plane PostgreSQL."""
    upload = await db.get(UploadMetadata, upload_id)
    if not upload:
        # Fallback para o LandingZoneManager se não localizado no DB
        upload = landing_zone_mgr.get_upload_by_id(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
        
    return {
        "upload_id": upload.upload_id,
        "filename": upload.filename,
        "status": upload.status,
        "size_bytes": upload.size_bytes,
        "processing_run_id": upload.processing_run_id
    }
