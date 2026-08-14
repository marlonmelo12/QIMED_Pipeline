"""
DAG: QIMED Silver Transformation Pipeline
Orchestrates the semantic normalization, terminology lookup, and entity resolution
from Bronze Delta tables to canonical Silver Delta tables.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.logging_config import get_logger

logger = get_logger("dag_silver_transformation")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_transform_fhir(**context):
    """Transform Bronze FHIR synthetic data to Silver."""
    from src.silver.pipeline import SilverTransformationPipeline
    pipeline = SilverTransformationPipeline()
    try:
        dataset = pipeline.transform_bronze_table("fhir/synthetic", source_type="fhir_synthetic")
        logger.info(f"Successfully transformed FHIR to Silver: {dataset.summary()}")
        return dataset.summary()
    except FileNotFoundError:
        logger.warning("Bronze table fhir/synthetic not found, skipping.")
        return {"skipped": True}


def task_transform_sih(**context):
    """Transform Bronze SIH data to Silver."""
    from src.silver.pipeline import SilverTransformationPipeline
    pipeline = SilverTransformationPipeline()
    try:
        dataset = pipeline.transform_bronze_table("datasus/sih", source_type="datasus_sih")
        logger.info(f"Successfully transformed SIH to Silver: {dataset.summary()}")
        return dataset.summary()
    except FileNotFoundError:
        logger.warning("Bronze table datasus/sih not found, skipping.")
        return {"skipped": True}


def task_transform_cnes(**context):
    """Transform Bronze CNES data to Silver."""
    from src.silver.pipeline import SilverTransformationPipeline
    pipeline = SilverTransformationPipeline()
    try:
        dataset = pipeline.transform_bronze_table("datasus/cnes", source_type="datasus_cnes")
        logger.info(f"Successfully transformed CNES to Silver: {dataset.summary()}")
        return dataset.summary()
    except FileNotFoundError:
        logger.warning("Bronze table datasus/cnes not found, skipping.")
        return {"skipped": True}


with DAG(
    dag_id="qimed_silver_transformation",
    default_args=default_args,
    description="Transforms Bronze Lakehouse data into FHIR R4 canonical Silver Delta tables",
    schedule_interval="@daily",
    catchup=False,
    tags=["silver", "normalization", "fhir", "lakehouse"],
) as dag:

    t_fhir = PythonOperator(
        task_id="transform_fhir_bronze_to_silver",
        python_callable=task_transform_fhir,
    )

    t_sih = PythonOperator(
        task_id="transform_sih_bronze_to_silver",
        python_callable=task_transform_sih,
    )

    t_cnes = PythonOperator(
        task_id="transform_cnes_bronze_to_silver",
        python_callable=task_transform_cnes,
    )

    [t_fhir, t_sih, t_cnes]
