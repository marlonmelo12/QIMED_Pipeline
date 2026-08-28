"""
Módulo de Pseudoanonimização Criptográfica (LGPD Gate) para o QIMED.
Aplica hashing criptográfico SHA-256 com Salt dinâmico em campos de PII.
"""
import os
import hashlib
from typing import List, Union, Tuple, Dict, Any, Optional
import pandas as pd
import polars as pl

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class Anonymizer:
    """
    Componente do Portal LGPD (LGPD Gate) para pseudoanonimizar campos PII.
    Utiliza SHA-256 combinado com um Salt configurável para hashing determinístico.
    """

    def __init__(self, salt: str = None):
        """
        Inicializa o anonimizador com um segredo Salt.
        Lê da variável de ambiente SALT_SECRET se não fornecido.
        """
        self.salt = salt or os.getenv("SALT_SECRET")
        if not self.salt:
            logger.warning("SALT_SECRET não está definido! Usando salt padrão não seguro. "
                           "NÃO UTILIZAR EM PRODUÇÃO.")
            self.salt = "default_unsecure_salt_replace_me"

    def _hash_value(self, value: Any) -> str:
        """
        Gera um hash SHA-256 consistente para um determinado valor combinado com o Salt.
        """
        if pd.isna(value) or value is None or str(value).strip() == "":
            return None
        
        val_str = str(value).strip()
        salted_val = f"{val_str}{self.salt}".encode('utf-8')
        return hashlib.sha256(salted_val).hexdigest()

    def generate_consistent_hash(self, value: Any) -> str:
        """
        Método público para gerar um hash consistente, útil para o linkage do Master Patient Index (MPI).
        """
        return self._hash_value(value)

    def anonymize(self, df: Union[pd.DataFrame, pl.DataFrame], pii_fields: List[str]) -> Tuple[Union[pd.DataFrame, pl.DataFrame], Dict[str, Any]]:
        """
        Aplica pseudoanonimização SHA-256 nos campos PII especificados no DataFrame.
        
        Argumentos:
            df: O DataFrame a ser anonimizado (pandas ou polars).
            pii_fields: Lista de colunas contendo PII a serem transformadas em hash.
            
        Retorna:
            Tupla contendo o DataFrame anonimizado e um dicionário de auditoria.
        """
        if not pii_fields:
            return df, {"status": "no_pii_fields_provided", "anonymized_columns": []}

        audit_log = {
            "status": "success",
            "anonymized_columns": []
        }

        # Suporte a Polars
        is_polars = isinstance(df, pl.DataFrame)
        if is_polars:
            pdf = df.to_pandas()
        elif isinstance(df, pd.DataFrame):
            pdf = df.copy()
        else:
            raise ValueError("Os dados de entrada devem ser um DataFrame pandas ou polars.")

        # Aplicação do hashing SHA-256
        for field in pii_fields:
            if field in pdf.columns:
                logger.info(f"Anonimizando campo sensível: {field}")
                pdf[field] = pdf[field].apply(self._hash_value)
                audit_log["anonymized_columns"].append(field)
            else:
                logger.warning(f"Campo PII '{field}' não encontrado no DataFrame.")

        # Converte de volta para polars se necessário
        result_df = pl.from_pandas(pdf) if is_polars else pdf

        return result_df, audit_log

    def anonymize_dataframe(self, df: pd.DataFrame, pii_fields: Optional[List[str]] = None) -> pd.DataFrame:
        """Anonimiza colunas sensíveis automaticamente."""
        if pii_fields is None:
            pii_fields = [c for c in ["CPF", "NOME", "CNS", "NU_CPF", "NO_PACIENTE", "NUM_CARTAO"] if c in df.columns]
        anonymized_df, _ = self.anonymize(df, pii_fields)
        return anonymized_df


# Alias para retrocompatibilidade
LGPDAnonymizer = Anonymizer
