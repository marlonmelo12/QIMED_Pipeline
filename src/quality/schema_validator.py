"""
Schema Validator & Schema Drift Detection - QIMED Lakehouse V3.
"""
from typing import Any, Dict, List, Optional, Set
import pyarrow as pa
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class SchemaValidator:
    """
    Valida contratos de schema e detecta desvios (schema drift).
    """

    def __init__(self, expected_columns: List[str], table_name: str):
        self.expected_columns = set(expected_columns)
        self.table_name = table_name

    def validate(self, actual_columns: List[str]) -> Dict[str, Any]:
        actual_set = set(actual_columns)
        missing = self.expected_columns - actual_set
        unexpected = actual_set - self.expected_columns
        has_drift = len(unexpected) > 0 or len(missing) > 0

        if has_drift:
            logger.warning(
                f"[SCHEMA DRIFT DETECTED] Tabela {self.table_name}: "
                f"Faltantes={missing}, Inesperadas={unexpected}"
            )

        return {
            "is_valid": len(missing) == 0,
            "has_drift": has_drift,
            "missing_columns": list(missing),
            "unexpected_columns": list(unexpected),
        }
