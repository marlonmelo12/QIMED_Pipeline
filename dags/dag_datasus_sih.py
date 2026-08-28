"""
DAG do Airflow: Ingestao do SIH/DATASUS (Internacoes Hospitalares) para todos os estados (Julho).
Executa download FTP real, anonimizacao LGPD e gravacao Delta Lake na Camada Bronze.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.logging_config import get_logger

logger = get_logger("dag_datasus_sih")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "qimed_datasus_sih",
    default_args=default_args,
    description="Ingestao SIH DATASUS (27 UFs - Julho)",
    schedule_interval="@monthly",
    catchup=False,
    tags=["datasus", "sih", "bronze"]
)


def run_sih_ingestion(**context):
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    res = pipeline.execute_bronze_ingestion(target_month=7, target_year=2024)
    logger.info(f"Ingestao SIH concluida: {res.get('sih')}")
    return res


t_ingest_sih = PythonOperator(
    task_id="download_and_ingest_sih_27_ufs",
    python_callable=run_sih_ingestion,
    dag=dag
)
