"""
QIMED DataQore — Script de ingestão FHIR sintética para popular a Bronze.
Executa o pipeline completo: gerar → parse → PII detect → anonymize → validate → write Bronze.
"""
import os
import sys

# Ensure project root on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.collectors.fhir_collector import FhirSyntheticCollector
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.validators.fhir_validator import FhirValidator
from src.lakehouse.bronze_writer import BronzeWriter
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("run_fhir_ingestion")


def main():
    # Configuration
    lakehouse_path = os.path.join(os.path.dirname(__file__), "lakehouse", "bronze")
    os.makedirs(lakehouse_path, exist_ok=True)

    num_patients = 50
    encounters_per_patient = 3

    logger.info(f"Starting FHIR synthetic ingestion: {num_patients} patients, {encounters_per_patient} encounters each")

    # Initialize components
    collector = FhirSyntheticCollector(
        num_patients=num_patients,
        encounters_per_patient=encounters_per_patient,
    )
    pii_detector = PIIDetector()
    anonymizer = Anonymizer(salt="qimed_dev_salt_2026")
    validator = FhirValidator()
    writer = BronzeWriter(lakehouse_path=lakehouse_path)
    catalog = DatasetCatalog()

    # 1. Fetch (generate synthetic bundles)
    logger.info("Step 1: Generating FHIR Bundles...")
    bundles = collector.fetch()
    logger.info(f"Generated {len(bundles)} bundles")

    # 2. Parse (flatten to DataFrame)
    logger.info("Step 2: Parsing bundles into DataFrame...")
    df = collector.parse(bundles)
    logger.info(f"Parsed {len(df)} total resources")
    logger.info(f"Resource types: {df['resourceType'].value_counts().to_dict()}")

    # 3. PII Detection
    logger.info("Step 3: Detecting PII fields...")
    pii_fields = pii_detector.detect_pii_fields("fhir_synthetic", df)
    logger.info(f"PII fields found: {pii_fields}")

    # 4. Anonymize
    logger.info("Step 4: Anonymizing PII...")
    df_anon, audit_log = anonymizer.anonymize(df, pii_fields)
    logger.info(f"Anonymization audit: {audit_log}")

    # 5. Validate
    logger.info("Step 5: Validating FHIR resources...")
    result = validator.validate(df_anon)
    logger.info(f"Valid: {len(result.valid_df)} | Rejected: {len(result.rejected_df)}")

    if not result.rejected_df.empty:
        quarantine_path = os.path.join(
            os.path.dirname(__file__), "lakehouse", "bronze", "_quarantine", "fhir"
        )
        os.makedirs(quarantine_path, exist_ok=True)
        result.rejected_df.to_parquet(
            os.path.join(quarantine_path, "rejected.parquet"), index=False
        )
        logger.info(f"Rejected rows saved to quarantine: {quarantine_path}")

    # 6. Write to Bronze
    logger.info("Step 6: Writing to Bronze (Delta Lake)...")
    metadata = {
        "source": "fhir",
        "subsystem": "synthetic",
        "source_type": "fhir_synthetic",
        "source_file": "in_memory_generation",
    }
    write_stats = writer.write(result.valid_df, metadata)
    logger.info(f"Write stats: {write_stats}")

    # 7. Register in Catalog
    logger.info("Step 7: Registering in metadata catalog...")
    import hashlib
    schema_fp = hashlib.md5(str(list(result.valid_df.columns)).encode()).hexdigest()
    dataset_id = catalog.register_dataset(
        source_type="fhir_synthetic",
        partition_path=write_stats["table_path"],
        row_count=write_stats["rows_written"],
        schema_fingerprint=schema_fp,
        pii_anonymized=True,
    )
    logger.info(f"Registered dataset: {dataset_id}")

    # Summary
    print("\n" + "=" * 60)
    print("  QIMED — FHIR Synthetic Ingestion Complete")
    print("=" * 60)
    print(f"  Patients generated:  {num_patients}")
    print(f"  Total resources:     {len(df)}")
    print(f"  PII fields masked:   {len(pii_fields)}")
    print(f"  Valid rows in Bronze:{write_stats['rows_written']}")
    print(f"  Rejected rows:       {len(result.rejected_df)}")
    print(f"  Write duration:      {write_stats['duration_seconds']}s")
    print(f"  Delta table path:    {write_stats['table_path']}")
    print(f"  Dataset ID:          {dataset_id}")
    print("=" * 60)


if __name__ == "__main__":
    main()
