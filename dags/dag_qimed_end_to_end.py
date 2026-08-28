"""
DAG Master End-to-End do QIMED DataQore: Download -> Bronze -> Silver -> Gold.
Orquestra o download real dos arquivos do DATASUS (SIH, SIA, CNES) para todos os 27 estados (Julho),
persistencia em Delta Lake e geracao dos Data Marts no Data Warehouse.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.logging_config import get_logger

logger = get_logger("dag_qimed_end_to_end")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "qimed_master_pipeline_end_to_end",
    default_args=default_args,
    description="Pipeline Completo: Download DATASUS 27 Estados (Julho) -> Bronze -> Silver -> Gold DW",
    schedule_interval="@monthly",
    catchup=False,
    tags=["datasus", "lakehouse", "bronze", "silver", "gold", "dw"]
)


def task_download_and_ingest_bronze(**context):
    """
    Executa o download real do FTP DATASUS (SIH, SIA, CNES) para todas as 27 UFs
    e grava na Camada Bronze em Delta Lake com anonimizacao LGPD.
    O mes e ano de referencia sao extraidos da data de execucao do Airflow.
    """
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    execution_date = context["data_interval_start"]
    target_year = execution_date.year
    target_month = execution_date.month
    logger.info(f"Iniciando download DATASUS para {target_month:02d}/{target_year} (27 estados)...")
    res = pipeline.execute_bronze_ingestion(target_month=target_month, target_year=target_year)
    logger.info(f"Camada Bronze persistida com sucesso: {res}")
    return res


def task_process_silver_layer(**context):
    """
    Le a Camada Bronze real e aplica normalizacao canonica, resolucao de entidades (MPI)
    e mapeamento de terminologias (IBGE 5570 municipios e CID-10).
    """
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    logger.info("Iniciando transformacao da Camada Silver...")
    res = pipeline.execute_silver_transformation()
    logger.info(f"Camada Silver persistida com sucesso: {res}")
    return res


def task_process_gold_and_dw(**context):
    """
    Le a Camada Silver e constroi os Data Marts Nacional, Estadual e Municipal no DuckDB.
    O mes e ano de referencia sao extraidos da data de execucao do Airflow para rotular
    o campo 'periodo' no Data Mart Nacional.
    """
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    execution_date = context["data_interval_start"]
    target_year = execution_date.year
    target_month = execution_date.month
    logger.info(f"Iniciando agregacao Gold para {target_month:02d}/{target_year}...")
    res = pipeline.execute_gold_aggregation(target_month=target_month, target_year=target_year)
    logger.info(f"Camada Gold e Data Warehouse concluidos: {res}")
    return res


def task_notify_backend_sync(**context):
    """
    Dispara notificação reativa via Webhook para o backend da aplicação iniciar o espelhamento.
    """
    from src.observability.webhook_notifier import trigger_sync_webhook
    run_id = str(context.get("run_id", "manual_run"))
    return trigger_sync_webhook(
        dag_id="qimed_master_pipeline_end_to_end",
        run_id=run_id,
        tables_ready=[
            "vw_internacoes_consolidadas",
            "dm_glosas_auditoria",
            "dm_hospital_efficiency",
            "dm_patient_readmissions",
            "aud_alertas_anomalias",
            "dm_ans_glosas_operadoras",
        ],
        status="success",
    )


t_bronze = PythonOperator(
    task_id="ingest_datasus_bronze_27_ufs",
    python_callable=task_download_and_ingest_bronze,
    dag=dag
)

t_silver = PythonOperator(
    task_id="transform_silver_lakehouse",
    python_callable=task_process_silver_layer,
    dag=dag
)

t_gold = PythonOperator(
    task_id="aggregate_gold_data_marts_dw",
    python_callable=task_process_gold_and_dw,
    dag=dag
)

t_notify = PythonOperator(
    task_id="notify_backend_mirror_trigger",
    python_callable=task_notify_backend_sync,
    dag=dag
)

# Definicao de Dependencias
t_bronze >> t_silver >> t_gold >> t_notify
