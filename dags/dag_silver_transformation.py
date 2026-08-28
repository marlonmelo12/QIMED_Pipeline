"""
DAG do Airflow: Transformacao da Camada Silver do QIMED.
Orquestra a normalizacao canonica, mapeamento de terminologias e resolucao de entidades (MPI)
das tabelas Delta da camada Bronze para as tabelas canonicas da camada Silver.
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
    "start_date": datetime(2024, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "qimed_silver_transformation",
    default_args=default_args,
    description="Transformacao Canonica e Mapeamentos Semanticos da Camada Silver",
    schedule_interval="@monthly",
    catchup=False,
    tags=["silver", "normalization", "mpi", "lakehouse"]
)


def task_run_silver_pipeline(**context):
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    res = pipeline.execute_silver_transformation()
    logger.info(f"Transformacao Silver concluida com sucesso: {res}")
    return res


t_transform = PythonOperator(
    task_id="transform_canonical_silver_models",
    python_callable=task_run_silver_pipeline,
    dag=dag
)
