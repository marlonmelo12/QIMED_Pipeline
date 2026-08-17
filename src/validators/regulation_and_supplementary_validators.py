"""
Validadores de Estrutura e Qualidade para SISREG e ANS.
"""
from typing import List, Dict, Any
import pandas as pd
from src.validators.base import BaseValidator, ValidationResult
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class SisregValidator(BaseValidator):
    """Validador de qualidade e integridade para solicitacoes de regulacao SISREG / CROSS."""

    REQUIRED_COLUMNS = [
        "ID_SOLICITACAO",
        "CO_MUNICIPIO_IBGE",
        "DT_SOLICITACAO",
        "STATUS_REGULACAO"
    ]

    def __init__(self):
        super().__init__()

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if df is None or df.empty:
            logger.warning("DataFrame do SISREG esta vazio ou nulo.")
            return ValidationResult(pd.DataFrame(), pd.DataFrame(), {"reason": "DataFrame vazio"})

        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            logger.warning(f"Colunas obrigatorias ausentes no SISREG: {missing}")
            return ValidationResult(pd.DataFrame(columns=df.columns), df.copy(), {"reason": f"Missing columns: {missing}"})

        valid_df = df[df["ID_SOLICITACAO"].notnull()].copy()
        rejected_df = df[df["ID_SOLICITACAO"].isnull()].copy()

        logger.info(f"Validacao SISREG concluida com sucesso. Registros validos: {len(valid_df)}")
        return ValidationResult(
            valid_df=valid_df,
            rejected_df=rejected_df,
            stats={"valid_count": len(valid_df), "rejected_count": len(rejected_df)}
        )


class AnsValidator(BaseValidator):
    """Validador de qualidade e integridade para dados de Saude Suplementar da ANS."""

    REQUIRED_COLUMNS = [
        "CD_OPERADORA",
        "CD_MUNICIPIO_IBGE",
        "NR_BENEFICIARIOS_ATIVOS"
    ]

    def __init__(self):
        super().__init__()

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if df is None or df.empty:
            logger.warning("DataFrame da ANS esta vazio ou nulo.")
            return ValidationResult(pd.DataFrame(), pd.DataFrame(), {"reason": "DataFrame vazio"})

        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            logger.warning(f"Colunas obrigatorias ausentes na ANS: {missing}")
            return ValidationResult(pd.DataFrame(columns=df.columns), df.copy(), {"reason": f"Missing columns: {missing}"})

        valid_df = df[df["CD_OPERADORA"].notnull()].copy()
        rejected_df = df[df["CD_OPERADORA"].isnull()].copy()

        logger.info(f"Validacao ANS concluida com sucesso. Registros validos: {len(valid_df)}")
        return ValidationResult(
            valid_df=valid_df,
            rejected_df=rejected_df,
            stats={"valid_count": len(valid_df), "rejected_count": len(rejected_df)}
        )
