import os
from abc import ABC, abstractmethod
from typing import Dict, Any
import httpx

class BaseSecretProvider(ABC):
    """Interface abstrata para recuperação de credenciais de conexões."""
    
    @abstractmethod
    async def get_connection_credentials(self, connection_id: str) -> Dict[str, Any]:
        """Recupera as credenciais de uma conexão."""
        pass

class EnvSecretProvider(BaseSecretProvider):
    """Recupera credenciais a partir de variáveis de ambiente/ .env (Dev/Testes)."""
    
    async def get_connection_credentials(self, connection_id: str) -> Dict[str, Any]:
        # Formato esperado: QIMED_CONN_NOME_HOST
        prefix = f"QIMED_CONN_{connection_id.upper()}"
        return {
            "host": os.getenv(f"{prefix}_HOST"),
            "port": os.getenv(f"{prefix}_PORT"),
            "user": os.getenv(f"{prefix}_USER"),
            "password": os.getenv(f"{prefix}_PASS"),
            "database": os.getenv(f"{prefix}_DB"),
        }

class BackendHttpSecretProvider(BaseSecretProvider):
    """Recupera credenciais criptografadas via HTTP do backend (Prod)."""
    
    def __init__(self):
        self.backend_url = os.getenv("QIMED_BACKEND_URL", "http://localhost:8000")
        self.service_token = os.getenv("QIMED_SERVICE_TOKEN", "")

    async def get_connection_credentials(self, connection_id: str) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.backend_url}/api/v1/internal/conexoes/{connection_id}",
                headers={"Authorization": f"Bearer {self.service_token}"}
            )
            response.raise_for_status()
            return response.json()

def get_secret_provider() -> BaseSecretProvider:
    """Factory para instanciar o Secret Provider correto baseado no ambiente."""
    environment = os.getenv("ENVIRONMENT", "dev").lower()
    if environment == "prod":
        return BackendHttpSecretProvider()
    return EnvSecretProvider()

