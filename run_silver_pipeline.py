"""
QIMED DataQore — Standalone runner for Bronze -> Silver Transformation Pipeline.
Reads Bronze Delta Lake tables -> Normalizes Semantically -> Resolves Master IDs -> Persists Silver Delta tables.
"""
import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.silver.pipeline import SilverTransformationPipeline
from src.utils.logging_config import get_logger

logger = get_logger("run_silver_pipeline")


def main():
    print("=" * 65)
    print("  QIMED DataQore — Silver Transformation Pipeline (Bronze -> Silver)")
    print("=" * 65)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_path = os.path.join(base_dir, "lakehouse", "bronze")
    silver_path = os.path.join(base_dir, "lakehouse", "silver")

    pipeline = SilverTransformationPipeline(
        bronze_base_path=bronze_path,
        silver_base_path=silver_path
    )

    # 1. Transform Bronze FHIR Synthetic table
    print("\n[Step 1/3] Transforming Bronze FHIR Synthetic Table...")
    try:
        fhir_dataset = pipeline.transform_bronze_table("fhir/synthetic", source_type="fhir_synthetic")
        print("  ✅ FHIR Transformation Summary:")
        for entity, count in fhir_dataset.summary().items():
            print(f"     - {entity.capitalize()}: {count} rows")
    except Exception as e:
        print(f"  ❌ Error transforming FHIR: {e}")

    # 2. Check and transform Bronze SIH if available
    sih_bronze_path = os.path.join(bronze_path, "datasus", "sih")
    print("\n[Step 2/3] Checking Bronze DATASUS SIH Table...")
    if os.path.exists(sih_bronze_path):
        try:
            sih_dataset = pipeline.transform_bronze_table("datasus/sih", source_type="datasus_sih")
            print("  ✅ SIH Transformation Summary:")
            for entity, count in sih_dataset.summary().items():
                print(f"     - {entity.capitalize()}: {count} rows")
        except Exception as e:
            print(f"  ❌ Error transforming SIH: {e}")
    else:
        print("  ℹ️  No Bronze SIH table found yet (use Airflow DAG or DATASUS collector to ingest).")

    # 3. Check and transform Bronze CNES if available
    cnes_bronze_path = os.path.join(bronze_path, "datasus", "cnes")
    print("\n[Step 3/3] Checking Bronze DATASUS CNES Table...")
    if os.path.exists(cnes_bronze_path):
        try:
            cnes_dataset = pipeline.transform_bronze_table("datasus/cnes", source_type="datasus_cnes")
            print("  ✅ CNES Transformation Summary:")
            for entity, count in cnes_dataset.summary().items():
                print(f"     - {entity.capitalize()}: {count} rows")
        except Exception as e:
            print(f"  ❌ Error transforming CNES: {e}")
    else:
        print("  ℹ️  No Bronze CNES table found yet (use Airflow DAG or DATASUS collector to ingest).")

    print("\n" + "=" * 65)
    print("  Silver Layer Transformation Complete!")
    print(f"  Destination Path: {silver_path}")
    print("=" * 65)


if __name__ == "__main__":
    main()
