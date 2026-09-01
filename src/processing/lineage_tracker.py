"""
Data Lineage Tracker - Camada Sistema - QIMED Lakehouse V3.
Rastreia a genealogia ponta a ponta dos dados desde o DATASUS bruto
at? as tabelas canônicas Silver e Data Marts Gold em lakehouse/system/lineage.
"""
import os
import time
from typing import Any, Dict, List, Optional
import pandas as pd
from deltalake.writer import write_deltalake

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class DataLineageTracker:
    """
    Registra transições e transformações de linhagem na tabela Delta lakehouse/system/lineage.
    """

    def __init__(self, system_dir: Optional[str] = None):
        cfg = load_pipeline_config()
        base_system = system_dir or cfg.get("paths", {}).get("system_dir", "lakehouse/system")
        self.lineage_table_path = os.path.join(base_system, "lineage")
        if not str(self.lineage_table_path).startswith("s3://"):
            os.makedirs(self.lineage_table_path, exist_ok=True)

    def record_lineage(
        self,
        execution_id: str,
        source_layer: str,
        source_entity: str,
        target_layer: str,
        target_entity: str,
        rows_transformed: int,
        transformation_rules: Optional[str] = None,
    ):
        """
        Grava um nó de linhagem na tabela Delta.
        """
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        record = {
            "id_execucao": str(execution_id),
            "camada_origem": str(source_layer),
            "entidade_origem": str(source_entity),
            "camada_destino": str(target_layer),
            "entidade_destino": str(target_entity),
            "total_registros": int(rows_transformed),
            "regras_transformacao": str(transformation_rules or "SQL Transformation"),
            "timestamp_evento": str(now_str),
        }

        df = pd.DataFrame([record])
        try:
            write_deltalake(
                self.lineage_table_path,
                df,
                mode="append",
                schema_mode="merge",
            )
            logger.info(f"[LINEAGE] {source_entity} ({source_layer}) -> {target_entity} ({target_layer}) [{rows_transformed:,} linhas].")
        except Exception as e:
            logger.warning(f"Falha ao gravar linhagem Delta ({e}).")
