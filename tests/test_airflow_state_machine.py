import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient

from src.observability.airflow_callbacks import (
    notify_job_status_to_backend,
    on_dag_success_callback,
    on_dag_failure_callback,
    advance_pipeline_watermark,
    get_current_watermark,
)
from src.metadata.models import IngestionStrategy, JobStatus, Job, Run, PipelineState
from src.api.main import app

client = TestClient(app)


def test_notify_job_status_success():
    mock_context = {
        "dag_id": "qimed_master_pipeline_end_to_end",
        "run_id": "manual__2026-05-01",
        "conf": {
            "qimed_job_id": "job-test-123",
            "qimed_run_id": "run-test-456"
        },
        "duration": 42.5
    }

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        res = notify_job_status_to_backend(
            context=mock_context,
            status="SUCCEEDED",
            watermark="2026-05",
            rows_read=1000,
            rows_written=980
        )

        assert res["status"] == "delivered"
        assert res["http_code"] == 200
        mock_post.assert_called_once()
        
        payload = res["payload"]
        assert payload["job_id"] == "job-test-123"
        assert payload["run_id"] == "run-test-456"
        assert payload["status"] == "SUCCEEDED"
        assert payload["watermark"] == "2026-05"
        assert payload["rows_read"] == 1000
        assert payload["rows_written"] == 980
        assert payload["execution_time_seconds"] == 42.5


def test_notify_job_status_failure():
    mock_context = {
        "dag_id": "qimed_master_pipeline_end_to_end",
        "run_id": "manual__2026-05-01",
        "exception": RuntimeError("Erro ao conectar no FTP DATASUS"),
        "duration": 15.2
    }

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        res = on_dag_failure_callback(mock_context)

        assert res["status"] == "delivered"
        payload = res["payload"]
        assert payload["status"] == "FAILED"
        assert "Erro ao conectar no FTP DATASUS" in payload["error_message"]


def test_atomic_watermark_progression():
    pipeline_id = "test_pipeline_atomic"
    conn_id = "test_conn"
    source = "datasus"
    entity = "sih"

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        # 1. Estado inicial
        initial_wm = get_current_watermark(pipeline_id, conn_id, source, entity)
        assert initial_wm is None

        # 2. Execução 1 com sucesso
        state1 = advance_pipeline_watermark(
            pipeline_id=pipeline_id,
            connection_id=conn_id,
            source=source,
            entity=entity,
            new_watermark="2026-05",
            run_id="run_001"
        )
        assert state1["last_successful_watermark"] == "2026-05"
        assert get_current_watermark(pipeline_id, conn_id, source, entity) == "2026-05"
        assert mock_post.call_count == 1

        # 3. Execução 2 com sucesso
        state2 = advance_pipeline_watermark(
            pipeline_id=pipeline_id,
            connection_id=conn_id,
            source=source,
            entity=entity,
            new_watermark="2026-06",
            run_id="run_002"
        )
        assert state2["previous_watermark"] == "2026-05"
        assert state2["last_successful_watermark"] == "2026-06"
        assert get_current_watermark(pipeline_id, conn_id, source, entity) == "2026-06"
        assert mock_post.call_count == 2


def test_atomic_watermark_preservation_on_intermediate_failure():
    """
    Regras 21 e 35: Se uma task intermediária falhar, o watermark NÃO deve avançar,
    preservando o estado anterior para permitir retry sem gaps.
    """
    pipeline_id = "test_pipeline_fail_recovery"
    conn_id = "test_conn"
    source = "datasus"
    entity = "sih"

    with patch("requests.post"):
        # Watermark consolidado anterior
        advance_pipeline_watermark(
            pipeline_id=pipeline_id,
            connection_id=conn_id,
            source=source,
            entity=entity,
            new_watermark="2026-05",
            run_id="run_good"
        )

        # Simulação da execução para 2026-06:
        # Task 1 (Bronze) -> OK
        # Task 2 (Silver) -> FALHA (Exception lançada)
        # Task Final (Commit Watermark) -> NUNCA É EXECUTADA
        failed = False
        try:
            raise ValueError("Erro de validação clínica na Camada Silver")
        except Exception as e:
            failed = True
            mock_context = {
                "dag_id": pipeline_id,
                "run_id": "run_failed_002",
                "exception": e
            }
            on_dag_failure_callback(mock_context)

        assert failed is True

        # Valida que o watermark permaneceu no último bem-sucedido (2026-05), SEM avançar para 2026-06
        current_wm = get_current_watermark(pipeline_id, conn_id, source, entity)
        assert current_wm == "2026-05"


def test_api_pipeline_status_and_watermark_persistence():
    from src.metadata.database import get_db

    db_store = {}
    mock_db = AsyncMock()
    mock_job = Job(job_id="job-123", status=JobStatus.RUNNING, connection_id="conn1", pipeline_id="pipe1")
    mock_run = Run(run_id="run-123", job_id="job-123", status=JobStatus.RUNNING)

    async def mock_get(model, pk):
        if model == Job:
            return mock_job
        if model == Run:
            return mock_run
        return None

    async def mock_execute(stmt):
        mock_res = MagicMock()
        mock_res.scalars.return_value.first.side_effect = lambda: db_store.get("qimed_master_pipeline_end_to_end")
        return mock_res

    def mock_add(obj):
        if isinstance(obj, PipelineState):
            db_store[obj.pipeline_id] = obj

    mock_db.get = AsyncMock(side_effect=mock_get)
    mock_db.execute = AsyncMock(side_effect=mock_execute)
    mock_db.add = MagicMock(side_effect=mock_add)
    mock_db.commit = AsyncMock()

    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    payload = {
        "job_id": "job-123",
        "run_id": "run-123",
        "dag_id": "qimed_master_pipeline_end_to_end",
        "dag_run_id": "manual_123",
        "status": "SUCCEEDED",
        "watermark": "2026-07",
        "connection_id": "datasus_ftp",
        "source": "datasus",
        "entity": "sih",
        "execution_time_seconds": 35.8,
        "rows_read": 5000,
        "rows_written": 4990,
        "rows_rejected": 10
    }

    # 1. Envia status com Watermark via Webhook
    response = client.post("/api/v1/pipeline/status", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "recorded"
    assert mock_job.status == JobStatus.SUCCEEDED
    assert mock_run.status == JobStatus.SUCCEEDED
    assert mock_run.rows_written == 4990

    # 2. Valida persistência na tabela pipeline_state
    saved_state = db_store.get("qimed_master_pipeline_end_to_end")
    assert saved_state is not None
    assert saved_state.last_successful_watermark == "2026-07"
    assert saved_state.last_successful_run_id == "run-123"

    # 3. Consulta via GET /api/v1/pipeline/state/{pipeline_id}
    res_get = client.get("/api/v1/pipeline/state/qimed_master_pipeline_end_to_end")
    assert res_get.status_code == 200
    state_data = res_get.json()
    assert state_data["last_successful_watermark"] == "2026-07"
    assert state_data["pipeline_id"] == "qimed_master_pipeline_end_to_end"

    app.dependency_overrides = {}
