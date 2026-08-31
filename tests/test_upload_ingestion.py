import os
import io
import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from unittest.mock import patch, MagicMock, AsyncMock

client = TestClient(app)

@patch("src.api.routers.uploads.OrchestrationService")
def test_upload_accepted(mock_orch_service):
    from src.metadata.database import get_db
    mock_db = AsyncMock()
    mock_res = MagicMock()
    mock_res.scalars.return_value.first.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_res)
    mock_db.commit = AsyncMock()
    
    async def override_get_db():
        yield mock_db
        
    app.dependency_overrides[get_db] = override_get_db

    mock_orch_instance = MagicMock()
    mock_orch_instance.trigger_pipeline = AsyncMock(return_value="job-mock123")
    mock_orch_service.return_value = mock_orch_instance
    
    dummy_file = "dummy.csv"
    with open(dummy_file, "w") as f:
        f.write("id,nome\n1,Teste")
        
    try:
        with open(dummy_file, "rb") as f:
            response = client.post(
                "/api/v1/ingestion/upload",
                files={"file": ("dummy.csv", f, "text/csv")}
            )
        
        assert response.status_code == 202
        data = response.json()
        assert "upload_id" in data
        assert data["job_id"] == "job-mock123"
        assert data["status"] == "Accepted"
    finally:
        if os.path.exists(dummy_file):
            os.remove(dummy_file)
        app.dependency_overrides = {}

