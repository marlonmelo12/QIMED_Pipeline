"""
DAG do Airflow: Agregacao da Camada Gold e Carga do Data Warehouse DuckDB.
Gera os Data Marts Nacional, Estadual e Municipal para analise de ocupacao de leitos e glosas.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.logging_config import get_logger

logger = get_logger("dag_gold_aggregation")

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
    "qimed_gold_aggregation",
    default_args=default_args,
    description="Agregacao Gold e Geracao de KPIs no Data Warehouse",
    schedule_interval="@monthly",
    catchup=False,
    tags=["gold", "kpis", "dw", "duckdb", "analytics"]
)


def task_run_gold_pipeline(**context):
    from src.pipeline.master_pipeline import QimedMasterPipeline
    pipeline = QimedMasterPipeline()
    res = pipeline.execute_gold_aggregation()
    logger.info(f"Agregacao Gold e Data Warehouse concluidos com sucesso: {res}")
    return res


def task_notify_backend_sync(**context):
    """
    Notifica o backend que os Data Marts DuckDB Gold estão prontos para consulta OLAP.
    O backend deve invalidar cache de dashboards — NÃO espelhar dados no PostgreSQL.
    """
    from src.observability.webhook_notifier import trigger_sync_webhook
    run_id = str(context.get("run_id", "manual_run"))
    return trigger_sync_webhook(
        dag_id="qimed_gold_aggregation",
        run_id=run_id,
        tables_ready=[
            "vw_internacoes_consolidadas",
            "dm_glosas_auditoria",
            "dm_hospital_efficiency",
            "aud_alertas_anomalias",
        ],
        status="success",
    )


t_gold_agg = PythonOperator(
    task_id="build_gold_data_marts_and_dw_views",
    python_callable=task_run_gold_pipeline,
    dag=dag
)

t_notify = PythonOperator(
    task_id="notify_backend_pipeline_ready",
    python_callable=task_notify_backend_sync,
    dag=dag
)

t_gold_agg >> t_notify
