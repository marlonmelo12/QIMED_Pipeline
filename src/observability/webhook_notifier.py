"""
Módulo de Notificação Reativa via Webhook - QIMED Lakehouse V3.
Dispara chamadas HTTP POST assíncronas/reativas para o backend da aplicação
ao término bem-sucedido de etapas de ingestão, transformação Silver e agregação Gold.
"""
import os
import json
import requests
from typing import Any, Dict, List, Optional
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

DEFAULT_WEBHOOK_URL = "http://localhost:8000/api/v1/sync/mirror-trigger"


def trigger_sync_webhook(
    dag_id: str,
    run_id: str,
    tables_ready: Optional[List[str]] = None,
    status: str = "success",
    execution_id: Optional[str] = None,
    webhook_url: Optional[str] = None,
    timeout_seconds: int = 5,
    ignore_errors: bool = True
) -> Dict[str, Any]:
    """
    Envia notificação via HTTP POST para o endpoint do backend da aplicação.
    """
    url = webhook_url or os.getenv("BACKEND_SYNC_WEBHOOK_URL", DEFAULT_WEBHOOK_URL)
    payload = {
        "event": "PIPELINE_COMPLETED",
        "dag_id": dag_id,
        "dag_run_id": run_id,
        "status": status,
        "execution_id": execution_id or run_id,
        "tables_ready": tables_ready or [],
    }
    headers = {"Content-Type": "application/json"}
    
    logger.info(f"[WEBHOOK SYNC] Enviando notificacao reativa para {url} (DAG: {dag_id}, Run: {run_id})...")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
        logger.info(f"[WEBHOOK SYNC] Resposta do backend: HTTP {resp.status_code}")
        return {"status": "delivered", "http_code": resp.status_code, "url": url}
    except Exception as e:
        msg = f"[WEBHOOK SYNC] Backend indisponivel em {url}: {e}"
        if ignore_errors:
            logger.warning(msg)
            return {"status": "failed_silently", "error": str(e), "url": url}
        else:
            logger.error(msg)
            raise
