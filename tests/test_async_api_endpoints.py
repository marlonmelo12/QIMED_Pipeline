import io
import os
import shutil
import tempfile
import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient

from src.api.main import app
from src.metadata.models import UploadMetadata, JobStatus, Job, Run
from dags.dag_process_upload import task_extract_and_write_bronze, task_transform_silver_canonical

client = TestClient(app)


@pytest.fixture
def temp_lakehouse_dir():
    temp_dir = tempfile.mkdtemp(prefix="qimed_test_api_lake_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_post_upload_delegates_to_airflow_and_returns_202():
    """
    Regra 21: O endpoint POST /upload não executa processamento pesado no FastAPI.
    Ele salva na Landing Zone, registra no Control Plane e delega ao OrchestrationService (Airflow).
    """
    from src.metadata.database import get_db

    mock_db = AsyncMock()
    mock_res = MagicMock()
    mock_res.scalars.return_value.first.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_res)
    mock_db.commit = AsyncMock()

    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    file_bytes = b"CD_ATENDIMENTO,CD_CNES,DT_ENTRADA,VL_CONTA\n8899,7042671,2026-05-15,3500.00\n"

    with patch("src.orchestration.service.OrchestrationService.trigger_pipeline") as mock_trigger:
        mock_trigger.return_value = "job-airflow-delegated-123"

        response = client.post(
            "/api/v1/ingestion/upload",
            files={"file": ("lote_hospital_8899.csv", io.BytesIO(file_bytes), "text/csv")}
        )

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "Accepted"
        assert data["job_id"] == "job-airflow-delegated-123"
        assert "upload_id" in data

        # Garante que o OrchestrationService foi acionado para disparar a DAG no Airflow
        mock_trigger.assert_called_once()
        args, kwargs = mock_trigger.call_args
        assert kwargs["pipeline_id"] == "process_upload_dag"
        assert kwargs["connection_id"] == "local_upload"
        assert "file_path" in kwargs["conf"]

    app.dependency_overrides = {}


def test_get_upload_status():
    from src.metadata.database import get_db

    mock_meta = UploadMetadata(
        upload_id="upl-status-123",
        filename="lote_teste.csv",
        content_hash="hash123",
        size_bytes=1024,
        source="http_upload",
        status=JobStatus.SUCCEEDED,
        processing_run_id="job-status-123"
    )

    mock_db = AsyncMock()
    mock_db.get = AsyncMock(return_value=mock_meta)

    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    # 1. Caso existente
    response = client.get("/api/v1/ingestion/upload/upl-status-123/status")
    assert response.status_code == 200
    data = response.json()
    assert data["upload_id"] == "upl-status-123"
    assert data["filename"] == "lote_teste.csv"
    assert data["status"] == "SUCCEEDED"
    assert data["size_bytes"] == 1024
    assert data["processing_run_id"] == "job-status-123"

    # 2. Caso inexistente
    mock_db.get = AsyncMock(return_value=None)
    response_404 = client.get("/api/v1/ingestion/upload/upl-nao-existe/status")
    assert response_404.status_code == 404

    app.dependency_overrides = {}


def test_post_pipeline_trigger_with_watermark_timestamp():
    from src.metadata.database import get_db

    mock_db = AsyncMock()
    mock_db.commit = AsyncMock()

    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    payload = {
        "connection_id": "datasus_ftp_alagoas",
        "pipeline_id": "qimed_master_pipeline_end_to_end",
        "watermark_timestamp": "2026-05",
        "mode": "incremental"
    }

    with patch("src.orchestration.service.OrchestrationService.trigger_pipeline") as mock_trigger:
        mock_trigger.return_value = "job-trig-999"

        response = client.post("/api/v1/pipeline/trigger", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "Accepted"
        assert data["job_id"] == "job-trig-999"
        assert data["connection_id"] == "datasus_ftp_alagoas"
        assert data["watermark_timestamp"] == "2026-05"

    app.dependency_overrides = {}


def test_airflow_worker_upload_dag_execution(temp_lakehouse_dir):
    """
    Testa a execução no Airflow Worker das tasks de processamento do upload (Data Plane isolado).
    """
    csv_file = os.path.join(temp_lakehouse_dir, "dados_hospitalares.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("CD_ATENDIMENTO,CD_CNES,DT_ENTRADA,DT_ALTA,CD_CID_PRINCIPAL,VL_TOTAL_CONTA,IE_SEXO,NR_IDADE,IE_OBITO\n")
        f.write("AT_10,7042671,2026-05-01,2026-05-04,J18.0,2500.00,MASCULINO,50,0\n")

    xcom_dict = {}
    mock_ti = MagicMock()
    mock_ti.xcom_push.side_effect = lambda key, value: xcom_dict.update({key: value})
    mock_ti.xcom_pull.side_effect = lambda key, task_ids=None: xcom_dict.get(key)

    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "upload_id": "upl-worker-999",
        "file_path": csv_file,
        "source_format": "tasy"
    }

    context = {
        "ti": mock_ti,
        "dag_run": mock_dag_run
    }

    # 1. Executa task Bronze no worker
    bronze_res = task_extract_and_write_bronze(**context)
    assert os.path.exists(bronze_res)

    # 2. Executa task Silver no worker
    rows_silver = task_transform_silver_canonical(**context)
    assert rows_silver == 1
