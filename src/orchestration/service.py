import uuid
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from src.metadata.models import Job, Run, JobStatus, UploadMetadata
from src.orchestration.airflow_client import AirflowClient
import httpx

class OrchestrationService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = AirflowClient()

    async def trigger_pipeline(
        self, 
        pipeline_id: str, 
        connection_id: str, 
        conf: Dict[str, Any]
    ) -> str:
        # Create a new Job in Control Plane
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        job = Job(
            job_id=job_id,
            connection_id=connection_id,
            pipeline_id=pipeline_id,
            status=JobStatus.QUEUED
        )
        self.db.add(job)
        
        # Create a determinist run for this job
        run_id = f"run-{uuid.uuid4().hex[:8]}"
        run = Run(
            run_id=run_id,
            job_id=job_id,
            status=JobStatus.QUEUED
        )
        self.db.add(run)
        
        # Commit to the DB BEFORE calling Airflow, ensuring idempotency state exists
        await self.db.commit()

        # Build Airflow payload
        # Ensure idempotent trigger by tying Airflow dag_run_id to our job_id
        dag_run_id = f"qimed-{job_id}"
        
        # Add tracking IDs to conf so Airflow DAGs know who invoked them
        conf["qimed_job_id"] = job_id
        conf["qimed_run_id"] = run_id

        try:
            # Trigger via Airflow REST API
            await self.client.trigger_dag(
                dag_id=pipeline_id,
                dag_run_id=dag_run_id,
                conf=conf
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                # 409 Conflict means this dag_run_id already exists.
                # Since we just generated a new UUID, this shouldn't happen unless
                # there's a collision, but we handle it gracefully.
                pass
            else:
                # Other errors: we mark Job as FAILED
                job.status = JobStatus.FAILED
                run.status = JobStatus.FAILED
                run.error_message = str(e)
                await self.db.commit()
                raise e
        except Exception as e:
            job.status = JobStatus.FAILED
            run.status = JobStatus.FAILED
            run.error_message = str(e)
            await self.db.commit()
            raise e

        return job_id

