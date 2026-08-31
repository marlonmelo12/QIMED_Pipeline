"""
Módulo de Notificação de Pipeline Pronto — QIMED Lakehouse V3.

Dispara chamadas HTTP POST para o backend da aplicação ao término bem-sucedido
de etapas de ingestão, transformação Silver e agregação Gold.

# Fronteira Arquitetural (ADR-001)
# ─────────────────────────────────
# DuckDB Gold (warehouse/qimed_dw.duckdb) é a ÚNICA fonte da verdade para OLAP.
# Este webhook NÃO instrui o backend a espelhar tabelas Gold no PostgreSQL.
# Seu único papel é sinalizar que os Data Marts estão disponíveis para leitura
# direta no DuckDB, permitindo que o backend invalide cache ou atualize status
# de jobs — sem qualquer cópia de dados.
#
# PostgreSQL fica restrito ao escopo OLTP do Airflow:
#   autenticação, sessões, logs de execução de DAGs e RBAC.
"""
import os
import json
import requests
from typing import Any, Dict, List, Optional
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

# Endpoint que recebe o sinal de "pipeline concluído, Data Marts prontos para leitura".
# NÃO é um trigger de espelhamento — o backend deve consultar o DuckDB diretamente.
DEFAULT_WEBHOOK_URL = "http://localhost:8000/api/v1/pipeline-ready"


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
    Envia notificação HTTP POST ao backend informando que os Data Marts do DuckDB
    estão prontos para consulta OLAP direta.

    O backend NÃO deve copiar os dados para PostgreSQL. Deve apenas:
      - Invalidar cache de dashboards referentes às tabelas listadas em `tables_ready`.
      - Atualizar o status do job de pipeline para 'completed'.
      - Registrar o evento no log de auditoria OLTP (PostgreSQL).

    Parâmetros
    ----------
    dag_id : str
        Identificador da DAG que concluiu.
    run_id : str
        Identificador da execução (dag_run_id do Airflow).
    tables_ready : list[str]
        Nomes dos Data Marts DuckDB prontos para leitura (ex.: "dm_glosas_auditoria").
    status : str
        Status da execução: "success" | "partial" | "failed".
    execution_id : str, optional
        ID de rastreabilidade adicional.
    webhook_url : str, optional
        URL de destino. Padrão: $BACKEND_PIPELINE_READY_WEBHOOK_URL ou DEFAULT_WEBHOOK_URL.
    timeout_seconds : int
        Timeout HTTP. Padrão: 5s.
    ignore_errors : bool
        Se True (padrão), falhas de entrega são logadas como warning, não bloqueiam o pipeline.
    """
    url = webhook_url or os.getenv("BACKEND_PIPELINE_READY_WEBHOOK_URL", DEFAULT_WEBHOOK_URL)
    payload = {
        "event": "PIPELINE_READY",
        "dag_id": dag_id,
        "dag_run_id": run_id,
        "status": status,
        "execution_id": execution_id or run_id,
        "tables_ready": tables_ready or [],
        # Instrução explícita: o backend deve ler do DuckDB, não copiar para PostgreSQL
        "olap_source": "duckdb_gold",
        "action_required": "invalidate_dashboard_cache",
    }
    headers = {"Content-Type": "application/json"}

    logger.info(
        f"[PIPELINE READY] Notificando backend em {url} — "
        f"DAG: {dag_id}, Run: {run_id}, "
        f"Data Marts prontos: {tables_ready}"
    )
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
        logger.info(f"[PIPELINE READY] Backend confirmou: HTTP {resp.status_code}")
        return {"status": "delivered", "http_code": resp.status_code, "url": url}
    except Exception as e:
        msg = f"[PIPELINE READY] Backend indisponível em {url}: {e}"
        if ignore_errors:
            logger.warning(msg)
            return {"status": "failed_silently", "error": str(e), "url": url}
        else:
            logger.error(msg)
            raise
