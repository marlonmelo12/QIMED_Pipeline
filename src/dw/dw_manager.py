"""
Data Warehouse Manager - QIMED Lakehouse V3.
Gerencia consultas analíticas sobre o banco DuckDB warehouse/qimed_dw.duckdb.
"""
import os
import duckdb
from typing import Any, Dict, List, Optional
import pandas as pd

from src.utils.config_loader import load_pipeline_config


class DWManager:
    """
    Interface de acesso analítico ao Data Warehouse DuckDB.
    """

    def __init__(self, dw_path: Optional[str] = None, db_path: Optional[str] = None, in_memory: bool = False):
        cfg = load_pipeline_config()
        self.dw_path = db_path or dw_path or cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")
        if self.dw_path != ":memory:":
            os.makedirs(os.path.dirname(self.dw_path), exist_ok=True)
        self.conn = duckdb.connect(self.dw_path)

    def register_table_from_df(self, name: str, df: pd.DataFrame) -> None:
        """
        Registra ou substitui uma tabela física no DuckDB a partir de um DataFrame.
        """
        if df is not None and not df.empty:
            self.conn.register("_df", df)
            self.conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")

    def create_semantic_views(self) -> None:
        """
        Cria views semânticas no DuckDB DW se as tabelas base existirem.
        """
        tables = self.list_tables()
        if "fct_encounters" in tables:
            self.conn.execute("CREATE OR REPLACE VIEW vw_encounters AS SELECT * FROM fct_encounters")
        if "fct_internacao" in tables:
            from src.gold.models.views_semanticas import registrar_views_semanticas
            registrar_views_semanticas(self.conn)

    def query(self, sql: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL e retorna um DataFrame do pandas.
        """
        return self.conn.sql(sql).df()

    def query_df(self, sql: str) -> pd.DataFrame:
        """
        Alias para query(), executando SQL e retornando DataFrame.
        """
        return self.query(sql)

    def list_tables(self) -> List[str]:
        """
        Lista todas as tabelas e views registradas no banco DuckDB.
        """
        rows = self.conn.execute("SHOW TABLES;").fetchall()
        return [r[0] for r in rows]

    def close(self) -> None:
        """
        Fecha a conexão ativa com o DuckDB.
        """
        try:
            self.conn.close()
        except Exception:
            pass


DataWarehouseManager = DWManager
