"""
Bronze Writer - QIMED Lakehouse V3.
Gerencia a tabela Delta Lake Bronze preservando 100% dos nomes originais do DATASUS
e injetando metadados técnicos padronizados em português.
"""
import os
import time
from typing import Any, Dict, List, Optional
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class BronzeWriter:
    """
    Gerencia a camada Bronze no Delta Lake com suporte a particionamento (ano/mes/uf)
    e rastreabilidade técnica.
    """

    def __init__(self, bronze_path: Optional[str] = None, lakehouse_path: Optional[str] = None):
        cfg = load_pipeline_config()
        self.bronze_path = bronze_path or lakehouse_path or cfg.get("paths", {}).get("bronze_dir", "lakehouse/bronze")
        self.lakehouse_path = self.bronze_path
        os.makedirs(self.bronze_path, exist_ok=True)

    def get_table_path(self, subsystem: str) -> str:
        return os.path.join(self.bronze_path, "datasus", subsystem.lower())

    def get_delta_table(self, subsystem: str) -> Optional[DeltaTable]:
        t_path = self.get_table_path(subsystem)
        if os.path.exists(t_path) and os.path.exists(os.path.join(t_path, "_delta_log")):
            return DeltaTable(t_path)
        return None

    def write(self, df: pd.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Escreve um DataFrame na camada Bronze Delta Lake com injeção de metadados técnicos.
        """
        metadata = metadata or {}
        if df is None or df.empty:
            return {"status": "skipped_empty", "rows_written": 0, "table_path": ""}

        df_out = df.copy()
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        source_type = metadata.get("source_type", metadata.get("subsystem", "unknown"))
        source_file = metadata.get("source_file", "unknown")
        subsystem = metadata.get("subsystem", "sih")

        df_out["_ingested_at"] = now_iso
        df_out["_source_type"] = source_type
        df_out["_source_file"] = source_file

        if "year" not in df_out.columns:
            df_out["year"] = metadata.get("year", "2026")
        if "month" not in df_out.columns:
            df_out["month"] = metadata.get("month", "01")

        table_path = self.get_table_path(subsystem)
        os.makedirs(table_path, exist_ok=True)

        write_deltalake(
            table_path,
            df_out,
            mode="append",
            partition_by=["year", "month"] if "year" in df_out.columns and "month" in df_out.columns else None,
            schema_mode="merge",
        )

        return {
            "status": "success",
            "rows_written": len(df_out),
            "table_path": table_path,
        }
