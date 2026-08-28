"""
Silver Transformation Pipeline for QIMED DataQore.
Orchestrates reading from Bronze Delta Lake -> Semantic Normalization -> Entity Resolution -> Silver Delta persistence.
"""
import os
import hashlib
from typing import Dict, Any, Optional
import pandas as pd
from deltalake import DeltaTable

from src.processing.duckdb_engine import DuckDBEngine
from src.silver.mappers.base_mapper import CanonicalDataset
from src.silver.mappers.sih_mapper import SihSemanticMapper
from src.silver.mappers.cnes_mapper import CnesSemanticMapper
from src.silver.mappers.fhir_mapper import FhirSemanticMapper
from src.silver.mappers.sisreg_mapper import SisregMapper
from src.silver.mappers.ans_mapper import AnsMapper
from src.silver.entity_resolver import EntityResolver
from src.lakehouse.silver_writer import SilverWriter
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class SilverTransformationPipeline:
    """
    Orchestrates the end-to-end transformation of Bronze datasets into canonical Silver tables.
    """

    def __init__(self, bronze_base_path: str = None, silver_base_path: str = None, duck_engine: Optional[Any] = None):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.bronze_base_path = bronze_base_path or os.getenv(
            "LAKEHOUSE_PATH", os.path.join(base_dir, "lakehouse", "bronze")
        )
        self.silver_base_path = silver_base_path or os.getenv(
            "SILVER_LAKEHOUSE_PATH", os.path.join(base_dir, "lakehouse", "silver")
        )
        self.duck_engine = duck_engine or DuckDBEngine()
        self.resolver = EntityResolver()
        self.writer = SilverWriter(silver_base_path=self.silver_base_path)
        self.catalog = DatasetCatalog()

        self.sih_mapper = SihSemanticMapper()
        self.cnes_mapper = CnesSemanticMapper()
        self.fhir_mapper = FhirSemanticMapper()
        self.sisreg_mapper = SisregMapper()
        self.ans_mapper = AnsMapper()

    def transform_dataframe(self, df: pd.DataFrame, source_type: str, source_file: str = "stream") -> CanonicalDataset:
        """
        Transforms an in-memory DataFrame from a given source type into canonical Silver entities.
        """
        logger.info(f"Transforming {len(df)} rows from source '{source_type}'")

        if source_type in ("datasus_sih", "sih"):
            canonical = self.sih_mapper.map_to_canonical(df, {"source_file": source_file})
        elif source_type in ("datasus_cnes", "cnes"):
            canonical = self.cnes_mapper.map_to_canonical(df, {"source_file": source_file})
        elif source_type in ("sisreg_regulation", "sisreg"):
            canonical = self.sisreg_mapper.map_to_canonical(df, {"source_file": source_file})
        elif source_type in ("ans_data", "ans"):
            canonical = self.ans_mapper.map_to_canonical(df, {"source_file": source_file})
        elif "fhir" in source_type:
            canonical = self.fhir_mapper.map_to_canonical(df, {"source_file": source_file})
        else:
            raise ValueError(f"Unsupported source type for Silver transformation: {source_type}")

        # Run Entity Resolution (MPI/MFI)
        resolved_dataset = self.resolver.resolve(canonical)

        # Persist to Silver Delta tables
        write_results = self.writer.write_canonical_dataset(resolved_dataset)
        logger.info(f"Silver write results: {write_results}")

        # Register in catalog
        total_entities = sum(res.get("rows_written", 0) for res in write_results.values())
        self.catalog.register_dataset(
            source_type=f"silver_{source_type}",
            partition_path=self.silver_base_path,
            row_count=total_entities,
            schema_fingerprint=hashlib.md5(f"silver_{source_type}_{total_entities}".encode()).hexdigest(),
            pii_anonymized=True,
            extra_metadata={"summary": resolved_dataset.summary(), "tables": list(write_results.keys())}
        )

        return resolved_dataset

    def transform_bronze_table(self, relative_table_path: str, source_type: str) -> CanonicalDataset:
        """
        Reads a Bronze Delta table from disk and transforms it to Silver.
        Example relative_table_path: 'fhir/synthetic', 'datasus/sih', 'sisreg/regulation'
        """
        table_full_path = os.path.join(self.bronze_base_path, relative_table_path)
        if not os.path.exists(table_full_path):
            raise FileNotFoundError(f"Bronze table not found at {table_full_path}")

        logger.info(f"Reading Bronze Delta table from {table_full_path}")
        normalized_path = table_full_path.replace("\\", "/")
        query = f"SELECT * FROM delta_scan('{normalized_path}')"
        try:
            bronze_arrow = self.duck_engine.fetch_arrow(query)
            bronze_df = bronze_arrow.to_pandas()
        except Exception as e:
            logger.warning(
                f"[SILVER PIPELINE] delta_scan out-of-core DuckDB indisponivel para '{normalized_path}' ({e}). "
                f"Executando fallback controlado para DeltaTable."
            )
            dt = DeltaTable(table_full_path)
            bronze_df = dt.to_pandas()

        return self.transform_dataframe(bronze_df, source_type=source_type, source_file=relative_table_path)
