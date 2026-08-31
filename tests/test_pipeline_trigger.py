import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from unittest.mock import patch, MagicMock, AsyncMock

client = TestClient(app)

@patch("src.api.routers.triggers.OrchestrationService")
def test_trigger_pipeline(mock_orch_service):
    from src.metadata.database import get_db
    mock_db = AsyncMock()
    
    async def override_get_db():
        yield mock_db
        
    app.dependency_overrides[get_db] = override_get_db

    mock_orch_instance = MagicMock()
    mock_orch_instance.trigger_pipeline = AsyncMock(return_value="job-123456")
    mock_orch_service.return_value = mock_orch_instance

    response = client.post(
        "/api/v1/pipeline/trigger",
        json={
            "pipeline_id": "dag_datasus_sih",
            "connection_id": "datasus_ftp",
            "watermark": "2026-05",
            "mode": "incremental"
        }
    )
    
    assert response.status_code == 202
    data = response.json()
    assert data["job_id"] == "job-123456"
    assert data["status"] == "Accepted"
    
    app.dependency_overrides = {}

