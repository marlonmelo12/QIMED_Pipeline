from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pyarrow as pa
from src.metadata.models import IngestionStrategy


class BaseIngestionAdapter(ABC):
    """
    Interface abstrata para ingestão desacoplada de dados na camada Bronze (Lakehouse Data Plane).
    Preserva fielmente a estrutura original (As-Is / Raw) da fonte.
    """

    @abstractmethod
    async def test_connection(self) -> bool:
        """Testa a conectividade com a fonte de dados."""
        pass

    @abstractmethod
    async def discover_schema(self) -> Dict[str, Any]:
        """Descobre o schema da fonte dinamicamente (para auditoria e drift)."""
        pass

    @abstractmethod
    def get_incremental_strategy(self) -> IngestionStrategy:
        """Retorna a estratégia suportada (ex: TIMESTAMP, FULL_SNAPSHOT, FILE_MANIFEST)."""
        pass

    @abstractmethod
    async def extract(self) -> pa.Table:
        """Extrai todos os dados brutos como uma Apache Arrow Table (Full Load)."""
        pass

    @abstractmethod
    async def extract_incremental(self, watermark: str) -> pa.Table:
        """Extrai apenas os dados modificados após o watermark como uma Apache Arrow Table."""
        pass

    @abstractmethod
    async def write_bronze(self, data: Any, destination_path: str) -> str:
        """
        Escreve os dados brutos extraídos em lakehouse/bronze/{source}/{table}/
        preservando nomes de colunas e tipos originais.
        """
        pass

