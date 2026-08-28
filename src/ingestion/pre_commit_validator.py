"""
Pre-Commit Validator - QIMED Lakehouse V3.
Garante integridade de schema, contagem de linhas, colunas obrigat?rias
e consist?ncia m?nima antes de autorizar a grava??o no Delta Lake.
"""
import os
import hashlib
from typing import Any, Dict, List, Optional
import pyarrow.parquet as pq

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class PreCommitValidator:
    """
    Validador pr?-commit para a camada Bronze/Staging.
    """

    MANDATORY_COLUMNS = {
        "SIA": ["PA_CODUNI", "PA_PROC_ID", "PA_QTDPRO", "PA_VALPRO"],
        "SIH": ["N_AIH", "CNES", "DT_INTER", "DT_SAIDA", "VAL_TOT"],
        "CNES": ["CNES", "CODUFMUN"],
    }

    def __init__(self, subsystem: str, expected_schema: Optional[List[str]] = None):
        self.subsystem = subsystem.upper()
        self.expected_schema = expected_schema

    def validate_staging_files(
        self,
        staging_files: List[str],
        expected_min_rows: int = 1
    ) -> Dict[str, Any]:
        """
        Executa bateria de testes de sanidade nos arquivos de staging.
        """
        if not staging_files:
            return {
                "is_valid": False,
                "error": "Nenhum arquivo de staging fornecido para validacao.",
                "total_rows": 0,
            }

        total_rows = 0
        missing_mandatory_cols = set()
        detected_columns = set()

        for fpath in staging_files:
            if not os.path.exists(fpath) or os.path.getsize(fpath) == 0:
                return {
                    "is_valid": False,
                    "error": f"Arquivo de staging invalido ou vazio: {fpath}",
                    "total_rows": total_rows,
                }

            schema = pq.read_schema(fpath)
            col_names = set(schema.names)
            detected_columns.update(col_names)

            # Valida colunas obrigat?rias
            required = self.MANDATORY_COLUMNS.get(self.subsystem, [])
            for req in required:
                if req not in col_names:
                    missing_mandatory_cols.add(req)

            metadata = pq.read_metadata(fpath)
            total_rows += metadata.num_rows

        if total_rows < expected_min_rows:
            return {
                "is_valid": False,
                "error": f"Contagem de linhas ({total_rows}) menor que o esperado ({expected_min_rows}).",
                "total_rows": total_rows,
            }

        if missing_mandatory_cols:
            err_msg = f"Colunas obrigatorias ausentes: {sorted(missing_mandatory_cols)}"
            logger.error(f"[VALIDATOR] {err_msg} em {self.subsystem}")
            return {
                "is_valid": False,
                "error": err_msg,
                "total_rows": total_rows,
                "detected_columns": list(detected_columns),
                "missing_mandatory_columns": list(missing_mandatory_cols),
                "files_count": len(staging_files),
            }

        return {
            "is_valid": True,
            "total_rows": total_rows,
            "detected_columns": list(detected_columns),
            "missing_mandatory_columns": list(missing_mandatory_cols),
            "files_count": len(staging_files),
        }
