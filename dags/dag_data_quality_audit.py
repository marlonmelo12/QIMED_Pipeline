"""
DAG do Apache Airflow para Auditoria Forense Automatizada de Data Quality.
Valida integridade relacional, anomalias, volume e consistência de todas as camadas do Lakehouse e DW.
"""
from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.quality.data_quality import DataQualityAuditor
from src.utils.config_loader import load_pipeline_config
from src.utils.logging_config import get_logger

logger = get_logger("dag_data_quality_audit")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "qimed_data_quality_audit",
    default_args=default_args,
    description="Auditoria Forense de Data Quality no Lakehouse & Data Warehouse",
    schedule_interval="@daily",
    catchup=False,
    tags=["quality", "audit", "governance", "lakehouse", "dw"]
)


def run_full_warehouse_audit(**kwargs):
    """Executa a auditoria completa de Data Quality no DuckDB e Delta Lake."""
    cfg = load_pipeline_config()
    auditor = DataQualityAuditor(config=cfg)
    logger.info("Iniciando auditoria completa de Data Quality no Lakehouse & DW...")
    results = auditor.audit_full_warehouse()
    
    logger.info(f"Auditoria de Data Quality concluída: {results.get('summary', 'OK')}")
    
    # Salva relatório de auditoria em disco
    audit_report_path = os.path.join(cfg.get("paths", {}).get("lakehouse_root", "lakehouse"), "latest_quality_audit.json")
    try:
        with open(audit_report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Relatório de auditoria salvo em: {audit_report_path}")
    except Exception as e:
        logger.warning(f"Não foi possível salvar relatório de auditoria em disco: {e}")

    return results


t_audit = PythonOperator(
    task_id="audit_warehouse_data_quality",
    python_callable=run_full_warehouse_audit,
    dag=dag
)
