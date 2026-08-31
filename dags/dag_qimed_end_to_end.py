"""
DAG Master End-to-End do QIMED Lakehouse: Download -> Bronze -> Silver -> Gold.
Orquestra a ingestão de dados, persistência em Delta Lake, transformação canônica
e geração dos Data Marts no Data Warehouse.
Inclui Callbacks de Observabilidade (on_success / on_failure) e avanço atômico de watermarks.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.logging_config import get_logger
from src.observability.airflow_callbacks import (
    on_dag_success_callback,
    on_dag_failure_callback,
    advance_pipeline_watermark,
)
from src.metadata.models import IngestionStrategy

logger = get_logger("dag_qimed_end_to_end")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_dag_failure_callback,
}

dag = DAG(
    "qimed_master_pipeline_end_to_end",
    default_args=default_args,
    description="Pipeline Completo: Download DATASUS 27 Estados (Julho) -> Bronze -> Silver -> Gold DW",
    schedule_interval="@monthly",
    catchup=False,
    tags=["datasus", "lakehouse", "bronze", "silver", "gold", "dw"],
    on_success_callback=on_dag_success_callback,
    on_failure_callback=on_dag_failure_callback,
)


def task_download_and_ingest_bronze(**context):
    """
    Executa o download real do FTP DATASUS (SIH, SIA, CNES) para todas as 27 UFs
    e grava na Camada Bronze em Delta Lake com anonimizacao LGPD.
    O mes e ano de referencia sao extraidos da data de execucao do Airflow.
    """
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    # Tenta ler o watermark injetado via Trigger API (ex: "2026-05")
    conf = context.get("dag_run").conf if context.get("dag_run") else {}
    watermark_str = conf.get("watermark") if conf else None
    
    if watermark_str and len(watermark_str) == 7:
        target_year = int(watermark_str[:4])
        target_month = int(watermark_str[5:7])
    else:
        # Fallback para o agendador padrão do Airflow
        execution_date = context.get("data_interval_start") or datetime.now()
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
    # Tenta ler o watermark injetado via Trigger API (ex: "2026-05")
    conf = context.get("dag_run").conf if context.get("dag_run") else {}
    watermark_str = conf.get("watermark") if conf else None
    
    if watermark_str and len(watermark_str) == 7:
        target_year = int(watermark_str[:4])
        target_month = int(watermark_str[5:7])
    else:
        # Fallback para o agendador padrão do Airflow
        execution_date = context.get("data_interval_start") or datetime.now()
        target_year = execution_date.year
        target_month = execution_date.month
    logger.info(f"Iniciando agregacao Gold para {target_month:02d}/{target_year}...")
    res = pipeline.execute_gold_aggregation(target_month=target_month, target_year=target_year)
    logger.info(f"Camada Gold e Data Warehouse concluidos: {res}")
    return res


def task_commit_watermark_and_notify(**context):
    """
    Task Final Atômica:
    1. Avança atomicamente o watermark (pipeline_state) SOMENTE após o sucesso confirmado de Bronze, Silver e Gold.
    2. Notifica o backend via webhook informando que os Data Marts estão disponíveis.
    """
    # Tenta ler o watermark injetado via Trigger API (ex: "2026-05")
    conf = context.get("dag_run").conf if context.get("dag_run") else {}
    watermark_str = conf.get("watermark") if conf else None
    
    if watermark_str and len(watermark_str) == 7:
        target_year = int(watermark_str[:4])
        target_month = int(watermark_str[5:7])
    else:
        # Fallback para o agendador padrão do Airflow
        execution_date = context.get("data_interval_start") or datetime.now()
        target_year = execution_date.year
        target_month = execution_date.month
    new_watermark = f"{target_year:04d}-{target_month:02d}"
    run_id = str(context.get("run_id", "manual_run"))

    # 1. Avanço Atômico do Watermark no PostgreSQL Control Plane (Regras 16, 21 e 35)
    advance_pipeline_watermark(
        pipeline_id="qimed_master_pipeline_end_to_end",
        connection_id="datasus_ftp",
        source="datasus",
        entity="master_lakehouse",
        new_watermark=new_watermark,
        run_id=run_id,
        context=context,
        strategy=IngestionStrategy.TIMESTAMP
    )

    # 2. Notificação do Backend
    from src.observability.webhook_notifier import trigger_sync_webhook
    return trigger_sync_webhook(
        dag_id="qimed_master_pipeline_end_to_end",
        run_id=run_id,
        tables_ready=[
            "vw_internacoes_consolidadas",
            "dm_glosas_auditoria",
            "dm_hospital_efficiency",
            "dm_patient_readmissions",
            "dm_ans_glosas_operadoras",
            "aud_alertas_anomalias",
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
    task_id="notify_backend_pipeline_ready",
    python_callable=task_commit_watermark_and_notify,
    dag=dag
)

# Definicao de Dependencias
t_bronze >> t_silver >> t_gold >> t_notify
