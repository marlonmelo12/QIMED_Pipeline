import os
import hashlib
from typing import List, Union, Tuple, Dict, Any
import pandas as pd
import polars as pl

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class Anonymizer:
    """
    LGPD Gate component for pseudoanonymizing PII fields.
    Uses SHA-256 with a configurable salt for consistent hashing.
    """

    def __init__(self, salt: str = None):
        """
        Initialize the Anonymizer with a salt.
        Reads from SALT_SECRET env var if not provided.
        """
        self.salt = salt or os.getenv("SALT_SECRET")
        if not self.salt:
            logger.warning("SALT_SECRET is not set! Using default unsecure salt. "
                           "DO NOT USE IN PRODUCTION.")
            self.salt = "default_unsecure_salt_replace_me"

    def _hash_value(self, value: Any) -> str:
        """
        Generate a consistent SHA-256 hash for a given value combined with the salt.
        """
        if pd.isna(value) or value is None or str(value).strip() == "":
            return None
        
        val_str = str(value).strip()
        salted_val = f"{val_str}{self.salt}".encode('utf-8')
        return hashlib.sha256(salted_val).hexdigest()

    def generate_consistent_hash(self, value: Any) -> str:
        """
        Public method to generate a consistent hash, useful for patient linking.
        """
        return self._hash_value(value)

    def anonymize(self, df: Union[pd.DataFrame, pl.DataFrame], pii_fields: List[str]) -> Tuple[Union[pd.DataFrame, pl.DataFrame], Dict[str, Any]]:
        """
        Apply SHA-256 pseudoanonymization to the specified PII fields in a DataFrame.
        
        Args:
            df: The DataFrame to anonymize (pandas or polars).
            pii_fields: List of columns containing PII to be hashed.
            
        Returns:
            Tuple containing the anonymized DataFrame and an audit log dictionary.
        """
        if not pii_fields:
            return df, {"status": "no_pii_fields_provided", "anonymized_columns": []}

        audit_log = {
            "status": "success",
            "anonymized_columns": []
        }

        # Handle Polars
        is_polars = isinstance(df, pl.DataFrame)
        if is_polars:
            pdf = df.to_pandas()
        elif isinstance(df, pd.DataFrame):
            pdf = df.copy()
        else:
            raise ValueError("Input data must be a pandas or polars DataFrame.")

        # Apply hashing
        for field in pii_fields:
            if field in pdf.columns:
                logger.info(f"Anonymizing field: {field}")
                pdf[field] = pdf[field].apply(self._hash_value)
                audit_log["anonymized_columns"].append(field)
            else:
                logger.warning(f"PII field '{field}' not found in DataFrame.")

        # Convert back to polars if necessary
        result_df = pl.from_pandas(pdf) if is_polars else pdf

        return result_df, audit_log
