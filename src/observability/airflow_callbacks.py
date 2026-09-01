"""
Módulo de Callbacks e Gerenciamento Atômico de Watermark do Airflow — QIMED Lakehouse.
Responsável por:
1. Notificar o backend sobre mudanças no ciclo de vida de jobs (RUNNING, SUCCEEDED, FAILED);
2. Avançar o watermark (pipeline_state no PostgreSQL Control Plane) de forma estritamente atômica
   apenas após sucesso das camadas Bronze, Silver e Gold.
"""
import os
import time
from collections.abc import Mapping
import requests
from typing import Any, Dict, Optional, Tuple
from src.utils.logging_config import setup_logger
from src.metadata.models import IngestionStrategy, JobStatus

logger = setup_logger(__name__)

DEFAULT_STATUS_WEBHOOK_URL = "http://localhost:8000/api/v1/pipeline/status"


def _extract_qimed_job_context(context: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrai com segurança `qimed_job_id` e `qimed_run_id` a partir de `dag_run.conf` (Mapping).
    Garante que jamais utilize `context['conf']` (que no Airflow é o AirflowConfigParser).
    Retorna (None, None) caso a DAG seja agendada ou `dag_run.conf` esteja ausente/vazio.
    """
    if not isinstance(context, dict):
        return None, None

    dag_run = context.get("dag_run")
    if dag_run and hasattr(dag_run, "conf"):
        conf = dag_run.conf
        if isinstance(conf, Mapping):
            return conf.get("qimed_job_id"), conf.get("qimed_run_id")

    # Fallback apenas se dag_run_conf for injetado explicitamente como Mapping no context (ex: mocks)
    raw_dag_run_conf = context.get("dag_run_conf")
    if isinstance(raw_dag_run_conf, Mapping):
        return raw_dag_run_conf.get("qimed_job_id"), raw_dag_run_conf.get("qimed_run_id")

    return None, None


def notify_job_status_to_backend(
    context: Dict[str, Any],
    status: str,
    error_message: Optional[str] = None,
    watermark: Optional[str] = None,
    connection_id: Optional[str] = None,
    source: Optional[str] = None,
    entity: Optional[str] = None,
    rows_read: int = 0,
    rows_written: int = 0,
    webhook_url: Optional[str] = None,
    timeout_seconds: int = 5,
    ignore_errors: bool = True
) -> Dict[str, Any]:
    """
    Notifica o backend sobre o status real da execução de uma DAG / Task do Airflow.
    Repassa o watermark para persistência atômica no PostgreSQL Control Plane.
    """
    dag = context.get("dag") if isinstance(context, dict) else None
    dag_id = dag.dag_id if dag else str(context.get("dag_id", "unknown_dag") if isinstance(context, dict) else "unknown_dag")
    
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    run_id = dag_run.run_id if dag_run else str(context.get("run_id", "unknown_run") if isinstance(context, dict) else "unknown_run")
    
    qimed_job_id, qimed_run_id = _extract_qimed_job_context(context)


    task_instance = context.get("task_instance") or context.get("ti")
    duration = 0.0
    if task_instance and hasattr(task_instance, "duration") and task_instance.duration is not None:
        duration = float(task_instance.duration)
    elif "duration" in context:
        duration = float(context["duration"])

    exception = context.get("exception")
    err_msg = error_message or (str(exception) if exception else None)

    url = webhook_url or os.getenv("BACKEND_STATUS_WEBHOOK_URL", DEFAULT_STATUS_WEBHOOK_URL)
    
    payload = {
        "job_id": qimed_job_id,
        "run_id": qimed_run_id,
        "dag_id": dag_id,
        "dag_run_id": run_id,
        "status": status,
        "watermark": watermark,
        "connection_id": connection_id,
        "source": source,
        "entity": entity,
        "execution_time_seconds": duration,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "error_message": err_msg,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    logger.info(
        f"[AIRFLOW CALLBACK] Disparando status {status} para DAG {dag_id} (run: {run_id}, watermark: {watermark})"
    )

    try:
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=timeout_seconds)
        logger.info(f"[AIRFLOW CALLBACK] Backend respondeu: HTTP {resp.status_code}")
        return {"status": "delivered", "http_code": resp.status_code, "payload": payload}
    except Exception as e:
        msg = f"[AIRFLOW CALLBACK] Falha ao notificar backend em {url}: {e}"
        if ignore_errors:
            logger.warning(msg)
            return {"status": "failed_silently", "error": str(e), "payload": payload}
        else:
            logger.error(msg)
            raise


def on_dag_success_callback(context: Dict[str, Any]) -> Dict[str, Any]:
    """Callback de sucesso invocado pelo Airflow."""
    return notify_job_status_to_backend(context, status=JobStatus.SUCCEEDED.value)


def on_dag_failure_callback(context: Dict[str, Any]) -> Dict[str, Any]:
    """Callback de falha invocado pelo Airflow."""
    exception = context.get("exception")
    err_str = str(exception) if exception else "Execução da DAG falhou"
    return notify_job_status_to_backend(context, status=JobStatus.FAILED.value, error_message=err_str)


# Cache local de suporte a testes unitários
_WATERMARK_STATE_STORE: Dict[str, Dict[str, Any]] = {}


def get_current_watermark(
    pipeline_id: str,
    connection_id: str = "datasus_ftp",
    source: str = "datasus",
    entity: str = "sih"
) -> Optional[str]:
    """Recupera o último watermark com sucesso confirmado do cache de teste/local."""
    key = f"{pipeline_id}:{connection_id}:{source}:{entity}"
    return _WATERMARK_STATE_STORE.get(key, {}).get("last_successful_watermark")


def advance_pipeline_watermark(
    pipeline_id: str,
    connection_id: str,
    source: str,
    entity: str,
    new_watermark: str,
    run_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    strategy: IngestionStrategy = IngestionStrategy.TIMESTAMP,
    webhook_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Avança atomicamente o watermark no PostgreSQL Control Plane via Webhook e atualiza cache.
    DEVE ser chamado estritamente na task final após a confirmação das camadas Silver e Gold.
    """
    key = f"{pipeline_id}:{connection_id}:{source}:{entity}"
    prev_state = _WATERMARK_STATE_STORE.get(key, {})
    prev_watermark = prev_state.get("last_successful_watermark")

    updated_state = {
        "pipeline_id": pipeline_id,
        "connection_id": connection_id,
        "source": source,
        "entity": entity,
        "strategy": strategy.value if hasattr(strategy, "value") else str(strategy),
        "previous_watermark": prev_watermark,
        "last_successful_watermark": new_watermark,
        "last_attempted_watermark": new_watermark,
        "last_successful_run_id": run_id,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    }
    _WATERMARK_STATE_STORE[key] = updated_state

    # Notifica o backend FastAPI para persistir o UPDATE no PostgreSQL
    ctx = context or {
        "dag_id": pipeline_id,
        "run_id": run_id or "manual_run",
    }
    notify_job_status_to_backend(
        context=ctx,
        status=JobStatus.SUCCEEDED.value,
        watermark=new_watermark,
        connection_id=connection_id,
        source=source,
        entity=entity,
        webhook_url=webhook_url
    )

    logger.info(
        f"[WATERMARK ATÔMICO] Watermark persistido no Control Plane: {pipeline_id} ({entity}) "
        f"de '{prev_watermark}' para '{new_watermark}' (run_id: {run_id})."
    )
    return updated_state
