"""
Delta Manifest Manager - Camada Sistema - QIMED Lakehouse V3.
Armazena o hist?rico transacional de ingest?o na tabela Delta Lake lakehouse/system/manifest,
permitindo consultas anal?ticas via DuckDB/SQL, auditoria forense e recupera??o inteligente (Recovery).
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


class DeltaManifestManager:
    """
    Gerencia o manifesto de ingest?o na tabela Delta lakehouse/system/manifest.
    """

    def __init__(self, system_dir: Optional[str] = None):
        cfg = load_pipeline_config()
        base_system = system_dir or cfg.get("paths", {}).get("system_dir", "lakehouse/system")
        self.manifest_table_path = os.path.join(base_system, "manifest")
        os.makedirs(self.manifest_table_path, exist_ok=True)

    def is_partition_committed(self, subsystem: str, year: int, month: int, uf: str) -> bool:
        """
        Verifica se a parti??o j? foi commitada com sucesso no Delta Lake para permitir Recovery.
        """
        if not os.path.exists(self.manifest_table_path) or not os.path.exists(os.path.join(self.manifest_table_path, "_delta_log")):
            return False

        try:
            dt = DeltaTable(self.manifest_table_path)
            df = dt.to_pandas()
            if df.empty:
                return False

            match = df[
                (df["subsistema"] == subsystem.upper()) &
                (df["ano"] == int(year)) &
                (df["mes"] == int(month)) &
                (df["uf"] == uf.upper()) &
                (df["status"] == "committed") &
                (df["linhas_processadas"] > 0)
            ]
            return not match.empty
        except Exception as e:
            logger.warning(f"Aviso ao consultar manifesto de particao {subsystem}/{year}/{month}/{uf}: {e}")
            return False

    def record_manifest_entry(
        self,
        subsystem: str,
        year: int,
        month: int,
        uf: str,
        files: List[str],
        total_rows: int,
        status: str,
        execution_id: str,
        duration_seconds: float = 0.0,
        error_message: Optional[str] = None,
    ):
        """
        Grava ou atualiza uma entrada de auditoria no manifesto Delta Lake.
        """
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        records = []

        file_list = files if files else ["unknown"]
        for f in file_list:
            fname = os.path.basename(str(f))
            fsize = os.path.getsize(f) if os.path.exists(f) else 0
            entry = {
                "id_execucao": str(execution_id),
                "subsistema": str(subsystem).upper(),
                "ano": int(year),
                "mes": int(month),
                "uf": str(uf).upper(),
                "arquivo": str(fname),
                "tamanho_bytes": int(fsize),
                "linhas_processadas": int(total_rows),
                "status": str(status),
                "tempo_execucao_s": float(duration_seconds),
                "mensagem_erro": str(error_message or ""),
                "data_registro": str(now_str),
            }
            records.append(entry)

        df = pd.DataFrame(records)
        try:
            write_deltalake(
                self.manifest_table_path,
                df,
                mode="append",
                schema_mode="merge",
            )
            logger.info(f"[MANIFEST RECORDED] {subsystem}-{uf} ({status}, {total_rows:,} linhas) em system/manifest.")
        except Exception as e:
            logger.warning(f"Falha ao gravar no manifesto Delta ({e}).")
