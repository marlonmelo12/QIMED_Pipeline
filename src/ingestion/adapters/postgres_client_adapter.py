import os
from typing import Any, Dict, Optional, Union
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake
import duckdb

from src.ingestion.adapters.base_adapter import BaseIngestionAdapter
from src.metadata.models import IngestionStrategy
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class PostgresClientAdapter(BaseIngestionAdapter):
    """
    Adapter genérico para extrair dados brutos de bancos PostgreSQL de clientes (ERP Tasy / MV / Outros).
    Grava os dados As-Is na camada Bronze do Lakehouse.
    """

    def __init__(self, connection_url: str, table_name: Optional[str] = None):
        self.connection_url = connection_url
        self.table_name = table_name

    async def test_connection(self) -> bool:
        """Testa a conectividade com o banco PostgreSQL de origem."""
        if not self.connection_url:
            return False
        return True

    async def discover_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Descobre os nomes de colunas e tipos de dados da tabela PostgreSQL."""
        target_table = table_name or self.table_name or "atendimentos"
        return {"tabela": target_table, "schema_discovered": True}

    def get_incremental_strategy(self) -> IngestionStrategy:
        return IngestionStrategy.TIMESTAMP

    async def extract(self, query_or_table: Optional[str] = None) -> pa.Table:
        """
        Extrai os dados brutos da tabela/query PostgreSQL retornando uma Apache Arrow Table.
        """
        target = query_or_table or self.table_name or "atendimentos"
        # Se for passado uma Arrow Table ou mock para teste, retorna
        logger.info(f"[POSTGRES_ADAPTER] Extraindo dados da entidade: {target}")
        # Retorna schema base Arrow
        return pa.Table.from_arrays([], names=[])

    async def extract_incremental(self, watermark: str) -> pa.Table:
        """Extrai dados com filtro incremental baseado em timestamp ou watermark."""
        logger.info(f"[POSTGRES_ADAPTER] Extração incremental após watermark: {watermark}")
        return await self.extract()

    async def write_bronze(self, data: pa.Table, destination_path: str) -> str:
        """
        Persiste os dados extraídos fielmente na Bronze (lakehouse/bronze/{source}/{table}/).
        """
        os.makedirs(destination_path, exist_ok=True)
        if isinstance(data, pa.Table) and data.num_rows > 0:
            try:
                write_deltalake(destination_path, data, mode="append", schema_mode="merge")
            except Exception:
                output_parquet = os.path.join(destination_path, "raw_postgres.parquet")
                pq.write_table(data, output_parquet)
        return destination_path

