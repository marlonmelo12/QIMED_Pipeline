import os
import asyncio
import pytest
from unittest.mock import patch, MagicMock
from src.security.secrets import (
    EnvSecretProvider,
    BackendHttpSecretProvider,
    get_secret_provider
)

def test_env_secret_provider():
    provider = EnvSecretProvider()
    
    with patch.dict(os.environ, {
        "QIMED_CONN_HOSPITAL_A_HOST": "localhost",
        "QIMED_CONN_HOSPITAL_A_PORT": "5432",
        "QIMED_CONN_HOSPITAL_A_USER": "admin",
        "QIMED_CONN_HOSPITAL_A_PASS": "secret",
        "QIMED_CONN_HOSPITAL_A_DB": "hospital"
    }):
        creds = asyncio.run(provider.get_connection_credentials("hospital_a"))
        
        assert creds["host"] == "localhost"
        assert creds["port"] == "5432"
        assert creds["user"] == "admin"
        assert creds["password"] == "secret"
        assert creds["database"] == "hospital"

@patch("httpx.AsyncClient.get")
def test_backend_http_secret_provider(mock_get):
    # Mocking a resposta da chamada HTTP async
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "host": "remote_db",
        "port": "3306",
        "user": "remote_user"
    }
    
    # httpx.AsyncClient.get() e assincrono, entao mockamos o awaitavel
    async def mock_get_coroutine(*args, **kwargs):
        return mock_response
    mock_get.side_effect = mock_get_coroutine

    with patch.dict(os.environ, {
        "QIMED_BACKEND_URL": "http://api.qimed.com",
        "QIMED_SERVICE_TOKEN": "super_secret_token"
    }):
        provider = BackendHttpSecretProvider()
        
        creds = asyncio.run(provider.get_connection_credentials("remote_hosp"))
        
        assert creds["host"] == "remote_db"
        assert creds["port"] == "3306"
        assert creds["user"] == "remote_user"
        
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert args[0] == "http://api.qimed.com/api/v1/internal/conexoes/remote_hosp"
        assert kwargs["headers"]["Authorization"] == "Bearer super_secret_token"

def test_get_secret_provider_factory():
    # Dev
    with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
        provider = get_secret_provider()
        assert isinstance(provider, EnvSecretProvider)
    
    # Prod
    with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
        provider = get_secret_provider()
        assert isinstance(provider, BackendHttpSecretProvider)

