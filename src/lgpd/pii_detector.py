import os
import yaml
from typing import List, Union, Dict, Any
import pandas as pd
import polars as pl

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class PIIDetector:
    """
    LGPD Gate component for detecting Personally Identifiable Information (PII)
    based on a configurable manifest file.
    """

    def __init__(self, manifest_path: str = None):
        """
        Initialize the PIIDetector by loading the PII manifest.
        """
        if not manifest_path:
            # Default to config/pii_manifest.yaml relative to project root
            # Assume we are running from project root or find it
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            manifest_path = os.path.join(base_dir, "config", "pii_manifest.yaml")
        
        self.manifest_path = manifest_path
        self.pii_mappings = self._load_manifest()

    def _load_manifest(self) -> Dict[str, List[str]]:
        """
        Load the YAML manifest containing PII mappings.
        """
        if not os.path.exists(self.manifest_path):
            logger.error(f"PII manifest not found at {self.manifest_path}")
            return {}

        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                mappings = yaml.safe_load(f)
                logger.info(f"Loaded PII manifest with {len(mappings)} source types.")
                return mappings or {}
        except Exception as e:
            logger.error(f"Failed to load PII manifest: {e}")
            return {}

    def detect_pii_fields(self, source_type: str, data: Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]) -> List[str]:
        """
        Identify which columns in the provided data contain PII based on the manifest.
        
        Args:
            source_type: The type of data source (e.g., 'datasus_sih').
            data: The dataset (DataFrame or Dictionary).
            
        Returns:
            List of column/field names that contain PII.
        """
        if source_type not in self.pii_mappings:
            logger.warning(f"Source type '{source_type}' not found in PII manifest.")
            return []

        known_pii_fields = set(self.pii_mappings[source_type])
        data_fields = set()

        if isinstance(data, pd.DataFrame):
            data_fields = set(data.columns)
        elif isinstance(data, pl.DataFrame):
            data_fields = set(data.columns)
        elif isinstance(data, dict):
            data_fields = set(data.keys())
        else:
            logger.error(f"Unsupported data type for PII detection: {type(data)}")
            return []

        # Find intersection
        detected_fields = list(known_pii_fields.intersection(data_fields))
        
        if detected_fields:
            logger.info(f"Detected PII fields for {source_type}: {detected_fields}")
        else:
            logger.info(f"No PII fields detected for {source_type}.")
            
        return detected_fields
