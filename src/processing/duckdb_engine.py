"""
DuckDB Engine - Motor de Transformação Out-of-Core - QIMED Lakehouse V3.
Executa consultas e transformações diretamente sobre Delta Lake e Parquet via delta_scan/parquet_scan.
"""
import os
from typing import Any, Dict, List, Optional
import duckdb
import pyarrow as pa

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class DuckDBEngine:
    """
    Gerenciador de conexões e execução SQL out-of-core do DuckDB para transformações Lakehouse.
    """

    def __init__(self, db_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        duck_cfg = self.cfg.get("duckdb", {})
        self.memory_limit = duck_cfg.get("memory_limit", "8GB")
        self.threads = duck_cfg.get("threads", 8)
        self.temp_dir = self.cfg.get("paths", {}).get("temp_duckdb_dir", "lakehouse/temp_duckdb")
        os.makedirs(self.temp_dir, exist_ok=True)

        self.db_path = db_path or ":memory:"
        self.conn = duckdb.connect(self.db_path)
        self._configure_engine()

    def _configure_engine(self):
        """
        Configura parâmetros de performance, limites de memória e extensões.
        """
        self.conn.execute(f"SET memory_limit = '{self.memory_limit}';")
        self.conn.execute(f"SET threads = {self.threads};")
        self.conn.execute(f"SET temp_directory = '{self.temp_dir.replace(chr(92), '/')}';")
        self.conn.execute("SET preserve_insertion_order = false;")
        # [CORRECAO-21] Carregamento da extensao delta em dois estagios:
        # 1. Tenta apenas LOAD (ja instalada, sem rede) — caminho rapido.
        # 2. Se falhar, tenta INSTALL + LOAD (download + instalacao).
        # 3. Se ambos falharem, RuntimeError imediato com mensagem clara.
        # ANTES: except Exception: pass — falha silenciosa; a primeira delta_scan()
        #        causaria um erro criptico "Function delta_scan not found".
        try:
            self.conn.execute("LOAD delta;")
            logger.debug("[DuckDB] Extensao delta carregada com sucesso (LOAD).")
        except Exception:
            try:
                self.conn.execute("INSTALL delta; LOAD delta;")
                logger.info("[DuckDB] Extensao delta instalada e carregada (INSTALL + LOAD).")
            except Exception as e:
                raise RuntimeError(
                    f"[FATAL] Extensao delta do DuckDB indisponivel: {e}\n"
                    f"O pipeline nao pode continuar sem suporte a delta_scan().\n"
                    f"Verifique a versao do DuckDB e a conectividade com extensions.duckdb.org."
                ) from e

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """
        Executa uma consulta SQL retornando uma relação DuckDB lazy.
        """
        return self.conn.sql(sql)

    def execute_sql(self, sql: str):
        """
        Executa comando SQL DDL/DML.
        """
        self.conn.execute(sql)

    def fetch_arrow(self, sql: str) -> pa.Table:
        """
        Executa consulta SQL e retorna o resultado como uma Apache Arrow Table.
        """
        res = self.conn.sql(sql).arrow()
        if hasattr(res, "read_all"):
            return res.read_all()
        return res

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
