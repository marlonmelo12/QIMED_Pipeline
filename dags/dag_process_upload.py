"""
DAG Airflow para Processamento de Uploads Pontuais (Data Plane / Worker Isolado).
Executa a carga pesada de extração (FileUploadAdapter) e transformação canônica (CanonicalTransformer)
fora do processo FastAPI (Regra 21).
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake

from src.utils.logging_config import get_logger
from src.observability.airflow_callbacks import (
    on_dag_success_callback,
    on_dag_failure_callback,
    notify_job_status_to_backend,
)

logger = get_logger("dag_process_upload")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_dag_failure_callback,
}

dag = DAG(
    "process_upload_dag",
    default_args=default_args,
    description="Pipeline de Processamento de Arquivos Uploaded: Landing -> Bronze -> Silver",
    schedule_interval=None,  # Disparo on-demand via REST API
    catchup=False,
    tags=["upload", "lakehouse", "bronze", "silver"],
    on_success_callback=on_dag_success_callback,
    on_failure_callback=on_dag_failure_callback,
)


def task_extract_and_write_bronze(**context):
    """
    Executa a extração bruta via DuckDB/PyArrow e grava na camada Bronze As-Is.
    """
    from src.ingestion.adapters.file_upload_adapter import FileUploadAdapter
    import asyncio

    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    upload_id = conf.get("upload_id", "upl-default")
    file_path = conf.get("file_path")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo do upload não localizado para processamento: {file_path}")

    logger.info(f"Iniciando extração do upload {upload_id} a partir de {file_path}...")
    adapter = FileUploadAdapter(file_path=file_path)
    raw_arrow = asyncio.run(adapter.extract())

    bronze_dir = os.path.join("lakehouse", "bronze", "uploads", upload_id)
    asyncio.run(adapter.write_bronze(raw_arrow, bronze_dir))
    logger.info(f"Upload {upload_id} gravado na Bronze ({raw_arrow.num_rows} linhas).")
    
    # Repassa metadados via XCom
    context["ti"].xcom_push(key="upload_id", value=upload_id)
    context["ti"].xcom_push(key="file_path", value=file_path)
    context["ti"].xcom_push(key="bronze_dir", value=bronze_dir)
    context["ti"].xcom_push(key="source_format", value=conf.get("source_format", "generic_csv"))
    return bronze_dir


def task_transform_silver_canonical(**context):
    """
    Lê a Bronze e aplica mapeamento declarativo (De-Para), sanitização e MPI para a Silver fct_internacao.
    """
    from src.ingestion.adapters.file_upload_adapter import FileUploadAdapter
    from src.processing.mappers.canonical_transformer import CanonicalTransformer
    import asyncio

    ti = context["ti"]
    upload_id = ti.xcom_pull(key="upload_id", task_ids="extract_and_write_bronze")
    file_path = ti.xcom_pull(key="file_path", task_ids="extract_and_write_bronze")
    source_format = ti.xcom_pull(key="source_format", task_ids="extract_and_write_bronze") or "generic_csv"

    adapter = FileUploadAdapter(file_path=file_path)
    raw_arrow = asyncio.run(adapter.extract())

    transformer = CanonicalTransformer()
    canonical_table = transformer.transform_to_fct_internacao(raw_arrow, source_format=source_format)

    silver_dir = os.path.join("lakehouse", "silver", "fct_internacao")
    os.makedirs(silver_dir, exist_ok=True)
    try:
        write_deltalake(silver_dir, canonical_table, mode="append", schema_mode="merge")
    except Exception:
        out_pq = os.path.join(silver_dir, f"{upload_id}_canonical.parquet")
        pq.write_table(canonical_table, out_pq)

    logger.info(f"Camada Silver fct_internacao atualizada ({canonical_table.num_rows} linhas).")
    return canonical_table.num_rows


t_bronze = PythonOperator(
    task_id="extract_and_write_bronze",
    python_callable=task_extract_and_write_bronze,
    dag=dag
)

t_silver = PythonOperator(
    task_id="transform_silver_canonical",
    python_callable=task_transform_silver_canonical,
    dag=dag
)

t_bronze >> t_silver
