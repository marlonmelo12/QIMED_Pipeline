"""
Catálogo de Metadados e Linhagem para o QIMED DataQore.
Registra datasets ingeridos, contagem de registros, linhagem e conformidade LGPD.
"""
import os
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class DatasetCatalog:
    """
    Catálogo de metadados que registra datasets ingeridos em arquivo JSON.
    Rastreia linhagem, esquema básico e status de conformidade com a LGPD.
    """

    def __init__(self, catalog_path: str = None):
        """
        Inicializa o catálogo.
        Usa _metadata/catalog.json na raiz do projeto por padrão.
        """
        if not catalog_path:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metadata_dir = os.path.join(base_dir, "_metadata")
            os.makedirs(metadata_dir, exist_ok=True)
            catalog_path = os.path.join(metadata_dir, "catalog.json")
            
        self.catalog_path = catalog_path
        self._initialize_catalog()

    def _initialize_catalog(self):
        """Cria o arquivo de catálogo se ele não existir."""
        if not os.path.exists(self.catalog_path):
            try:
                with open(self.catalog_path, 'w', encoding='utf-8') as f:
                    json.dump({"datasets": []}, f, indent=4)
                logger.info(f"Catálogo de datasets inicializado em {self.catalog_path}")
            except Exception as e:
                logger.error(f"Falha ao inicializar catálogo: {e}")

    def _load_catalog(self) -> Dict[str, Any]:
        """Carrega o catálogo do disco."""
        try:
            with open(self.catalog_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Falha ao carregar catálogo: {e}")
            return {"datasets": []}

    def _save_catalog(self, data: Dict[str, Any]):
        """Salva o catálogo em disco."""
        try:
            with open(self.catalog_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Falha ao salvar catálogo: {e}")

    def register_dataset(self, 
                         source_type: str, 
                         partition_path: str, 
                         row_count: int, 
                         schema_fingerprint: str, 
                         pii_anonymized: bool,
                         extra_metadata: Dict[str, Any] = None) -> str:
        """
        Registra um dataset recém-ingerido no catálogo.
        
        Retorna:
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
        
        logger.info(f"Dataset {dataset_id} registrado para a fonte {source_type}")
        return dataset_id

    def list_datasets(self) -> List[Dict[str, Any]]:
        """Lista todos os datasets registrados no catálogo."""
        return self._load_catalog().get("datasets", [])

    def get_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Recupera metadados de um dataset específico pelo ID."""
        datasets = self.list_datasets()
        for ds in datasets:
            if ds.get("dataset_id") == dataset_id:
                return ds
        return None

    def get_datasets_by_source(self, source_type: str) -> List[Dict[str, Any]]:
        """Recupera todos os datasets de um tipo específico de fonte."""
        datasets = self.list_datasets()
        return [ds for ds in datasets if ds.get("source_type") == source_type]


# Alias para compatibilidade com DAGs do Airflow
MetadataCatalog = DatasetCatalog
