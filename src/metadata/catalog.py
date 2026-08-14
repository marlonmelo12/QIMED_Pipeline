import os
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class DatasetCatalog:
    """
    Minimal metadata catalog that registers ingested datasets in a JSON file.
    Tracks lineage, basic schema info, and LGPD compliance status.
    """

    def __init__(self, catalog_path: str = None):
        """
        Initialize the catalog.
        Uses _metadata/catalog.json in the project root by default.
        """
        if not catalog_path:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metadata_dir = os.path.join(base_dir, "_metadata")
            os.makedirs(metadata_dir, exist_ok=True)
            catalog_path = os.path.join(metadata_dir, "catalog.json")
            
        self.catalog_path = catalog_path
        self._initialize_catalog()

    def _initialize_catalog(self):
        """Create the catalog file if it doesn't exist."""
        if not os.path.exists(self.catalog_path):
            try:
                with open(self.catalog_path, 'w', encoding='utf-8') as f:
                    json.dump({"datasets": []}, f, indent=4)
                logger.info(f"Initialized new dataset catalog at {self.catalog_path}")
            except Exception as e:
                logger.error(f"Failed to initialize catalog: {e}")

    def _load_catalog(self) -> Dict[str, Any]:
        """Load the catalog from disk."""
        try:
            with open(self.catalog_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load catalog: {e}")
            return {"datasets": []}

    def _save_catalog(self, data: Dict[str, Any]):
        """Save the catalog to disk."""
        try:
            with open(self.catalog_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save catalog: {e}")

    def register_dataset(self, 
                         source_type: str, 
                         partition_path: str, 
                         row_count: int, 
                         schema_fingerprint: str, 
                         pii_anonymized: bool,
                         extra_metadata: Dict[str, Any] = None) -> str:
        """
        Register a newly ingested dataset into the catalog.
        
        Returns:
            dataset_id (str)
        """
        dataset_id = str(uuid.uuid4())
        entry = {
            "dataset_id": dataset_id,
            "source_type": source_type,
            "partition_path": partition_path,
            "row_count": row_count,
            "schema_fingerprint": schema_fingerprint,
            "ingested_at": datetime.utcnow().isoformat(),
            "pii_anonymized": pii_anonymized,
            "extra_metadata": extra_metadata or {}
        }

        catalog_data = self._load_catalog()
        catalog_data["datasets"].append(entry)
        self._save_catalog(catalog_data)
        
        logger.info(f"Registered dataset {dataset_id} for source {source_type}")
        return dataset_id

    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets in the catalog."""
        return self._load_catalog().get("datasets", [])

    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific dataset by ID."""
        datasets = self.list_datasets()
        for ds in datasets:
            if ds.get("dataset_id") == dataset_id:
                return ds
        return None

    def get_datasets_by_source(self, source_type: str) -> List[Dict[str, Any]]:
        """Retrieve all datasets for a specific source type."""
        datasets = self.list_datasets()
        return [ds for ds in datasets if ds.get("source_type") == source_type]
