import os
import io
import shutil
import hashlib
import tempfile
import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient

from src.ingestion.landing_zone import LandingZoneManager
from src.metadata.models import UploadMetadata, JobStatus
from src.api.main import app

client = TestClient(app)


@pytest.fixture
def temp_landing_dir():
    temp_dir = tempfile.mkdtemp(prefix="qimed_test_landing_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_save_upload_new_file_streaming(temp_landing_dir):
    manager = LandingZoneManager(landing_dir=temp_landing_dir)
    file_content = b"col1,col2,col3\n100,200,300\n400,500,600\n"
    filename = "dados_hospitalares.csv"
    stream = io.BytesIO(file_content)

    meta = asyncio.run(manager.save_upload(stream, filename))

    assert meta is not None
    assert meta.upload_id.startswith("upl-")
    assert meta.filename == filename
    assert meta.size_bytes == len(file_content)
    assert meta.content_hash == hashlib.sha256(file_content).hexdigest()
    assert meta.status == JobStatus.PENDING

    # Verifica arquivo persistido em disco
    saved_file_path = os.path.join(temp_landing_dir, f"{meta.upload_id}_{filename}")
    assert os.path.exists(saved_file_path)
    with open(saved_file_path, "rb") as f:
        assert f.read() == file_content


def test_save_upload_db_idempotency_query(temp_landing_dir):
    manager = LandingZoneManager(landing_dir=temp_landing_dir)
    file_content = b"conteudo_de_teste_com_persistencia_no_postgres_123"
    filename = "pacientes.csv"
    expected_hash = hashlib.sha256(file_content).hexdigest()

    # Cria mock de AsyncSession simulando registro existente no PostgreSQL
    existing_meta = UploadMetadata(
        upload_id="upl-dbexisting123",
        filename="pacientes_antigo.csv",
        content_hash=expected_hash,
        size_bytes=len(file_content),
        status=JobStatus.PENDING,
        processing_run_id="job-existing-001"
    )

    mock_db = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = existing_meta
    mock_db.execute = AsyncMock(return_value=mock_result)

    # Executa save_upload passando o stream e a sessao do banco
    stream = io.BytesIO(file_content)
    result_meta = asyncio.run(manager.save_upload(stream, filename, db=mock_db))

    # Garante que consultou o banco via SELECT
    mock_db.execute.assert_called_once()

    # Garante que retornou a instancia do banco sem gravar novo arquivo no disco
    assert result_meta.upload_id == "upl-dbexisting123"
    assert result_meta.processing_run_id == "job-existing-001"
    
    files_in_landing = os.listdir(temp_landing_dir)
    assert len(files_in_landing) == 0  # Nenhum arquivo duplicado gravado


def test_streaming_hash_chunks_large_payload(temp_landing_dir):
    manager = LandingZoneManager(landing_dir=temp_landing_dir)
    # Cria payload simulado de 2MB via stream
    chunk_payload = b"Y" * (1024 * 1024 * 2)
    stream = io.BytesIO(chunk_payload)

    computed_hash, total_size = manager.compute_hash_and_size(stream, chunk_size=65536)
    expected_hash = hashlib.sha256(chunk_payload).hexdigest()

    assert computed_hash == expected_hash
    assert total_size == len(chunk_payload)
    assert stream.tell() == 0  # Garante que resetou o ponteiro do stream


@patch("src.api.routers.uploads.OrchestrationService")
def test_api_upload_streaming_and_idempotency(mock_orch_service, temp_landing_dir):
    from src.metadata.database import get_db

    # Simula banco de dados inicialmente vazio e depois com o registro inserido
    db_store = {}

    mock_db = AsyncMock()

    async def mock_execute(stmt):
        mock_res = MagicMock()
        # Se consultar por content_hash, retorna do db_store
        mock_res.scalars.return_value.first.side_effect = lambda: list(db_store.values())[0] if db_store else None
        return mock_res

    def mock_add(obj):
        if isinstance(obj, UploadMetadata):
            db_store[obj.content_hash] = obj

    mock_db.execute = AsyncMock(side_effect=mock_execute)
    mock_db.add = MagicMock(side_effect=mock_add)
    mock_db.commit = AsyncMock()

    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    mock_orch_instance = MagicMock()
    mock_orch_instance.trigger_pipeline = AsyncMock(return_value="job-streaming-001")
    mock_orch_service.return_value = mock_orch_instance

    file_bytes = b"cpf_hash,valor,cid\nhash_streaming_999,3500.0,J18\n"

    # Envio 1
    response1 = client.post(
        "/api/v1/ingestion/upload",
        files={"file": ("lote_spooled.csv", io.BytesIO(file_bytes), "text/csv")}
    )
    assert response1.status_code == 202
    data1 = response1.json()
    assert data1["status"] == "Accepted"
    upload_id_1 = data1["upload_id"]
    job_id_1 = data1["job_id"]
    assert job_id_1 == "job-streaming-001"

    # Envio 2 (Mesmo arquivo) -> Deve consultar o Postgres e retornar o mesmo status sem novo trigger
    response2 = client.post(
        "/api/v1/ingestion/upload",
        files={"file": ("lote_spooled_retry.csv", io.BytesIO(file_bytes), "text/csv")}
    )
    assert response2.status_code == 202
    data2 = response2.json()
    assert data2["upload_id"] == upload_id_1
    assert data2["job_id"] == job_id_1

    # Garante que o orquestrador so foi acionado 1 vez
    assert mock_orch_instance.trigger_pipeline.call_count == 1

    app.dependency_overrides = {}

