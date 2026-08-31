import os
import duckdb
from typing import Any, Dict, Optional, Union
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake

from src.ingestion.adapters.base_adapter import BaseIngestionAdapter
from src.metadata.models import IngestionStrategy
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class FileUploadAdapter(BaseIngestionAdapter):
    """
    Adapter para ingestão de arquivos (CSV, Parquet) na camada Bronze (As-Is / Raw).
    Utiliza DuckDB com extração vetorizada para Apache Arrow Table sem conversão para Pandas.
    """

    def __init__(self, file_path: str, format: Optional[str] = None):
        self.file_path = file_path
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        ext = os.path.splitext(file_path)[1].lower()
        self.format = (format or ("parquet" if ext in [".parquet", ".pq"] else "csv")).lower()

    async def test_connection(self) -> bool:
        """Verifica se o arquivo de origem existe e pode ser acessado."""
        return os.path.exists(self.file_path) and os.path.getsize(self.file_path) >= 0

    async def discover_schema(self) -> Dict[str, Any]:
        """Descobre os nomes de colunas e tipos originais via DuckDB."""
        safe_path = self.file_path.replace(chr(92), "/")
        with duckdb.connect(":memory:") as con:
            if self.format == "parquet":
                query = f"DESCRIBE SELECT * FROM read_parquet('{safe_path}');"
            else:
                query = f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}');"
            
            res = con.execute(query).fetchall()
            return {col[0]: col[1] for col in res}

    def get_incremental_strategy(self) -> IngestionStrategy:
        return IngestionStrategy.FILE_MANIFEST

    async def extract(self) -> pa.Table:
        """
        Extrai os dados brutos da fonte como Apache Arrow Table usando DuckDB (.to_arrow_table()).
        """
        safe_path = self.file_path.replace(chr(92), "/")
        with duckdb.connect(":memory:") as con:
            if self.format == "parquet":
                sql = f"SELECT * FROM read_parquet('{safe_path}')"
            else:
                sql = f"SELECT * FROM read_csv_auto('{safe_path}')"
            
            arrow_res = con.execute(sql).arrow()
            if hasattr(arrow_res, "read_all"):
                arrow_table = arrow_res.read_all()
            else:
                arrow_table = arrow_res
                
            logger.info(
                f"[FILE_ADAPTER] Extração bruta concluída: {arrow_table.num_rows} linhas, "
                f"{arrow_table.num_columns} colunas de {self.file_path}"
            )
            return arrow_table

    async def extract_incremental(self, watermark: str) -> pa.Table:
        """Para arquivos pontuais, extrai o conteúdo do arquivo."""
        return await self.extract()

    async def write_bronze(self, data: Union[pa.Table, str], destination_path: str) -> str:
        """
        Grava os dados brutos em lakehouse/bronze/{source}/{table}/ preservando colunas e tipos originais.
        """
        os.makedirs(destination_path, exist_ok=True)
        
        arrow_table = data if isinstance(data, pa.Table) else await self.extract()
        
        try:
            write_deltalake(
                destination_path,
                arrow_table,
                mode="append",
                schema_mode="merge"
            )
            logger.info(f"[BRONZE WRITE] Gravado Delta Lake em {destination_path} ({arrow_table.num_rows} linhas).")
        except Exception as e:
            # Fallback para escrita direta Parquet se Delta nativo não estiver inicializado
            output_parquet = os.path.join(destination_path, "raw_data.parquet")
            pq.write_table(arrow_table, output_parquet)
            logger.info(f"[BRONZE WRITE] Gravado Parquet raw em {output_parquet} ({arrow_table.num_rows} linhas).")
            
        return destination_path

