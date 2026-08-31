"""
Testes Unitários do Módulo de Notificação de Pipeline Pronto — QIMED Lakehouse V3.

Verifica que o webhook notifica o backend corretamente sobre a disponibilidade
dos Data Marts DuckDB Gold para leitura OLAP direta. NÃO testa espelhamento
de dados no PostgreSQL (o qual foi descontinuado — ver ADR-001).
"""
import pytest
from unittest.mock import patch, MagicMock
from src.observability.webhook_notifier import trigger_sync_webhook, DEFAULT_WEBHOOK_URL


def test_webhook_successful_delivery():
    """Notificação com status 200 OK: backend confirmou recebimento."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200

    with patch("requests.post", return_value=mock_resp) as mock_post:
        res = trigger_sync_webhook(
            dag_id="qimed_master_pipeline_end_to_end",
            run_id="exec_test_123",
            tables_ready=["dm_glosas_auditoria", "dm_ans_glosas_operadoras"],
            status="success",
            webhook_url="http://test-backend:8000/api/v1/pipeline-ready"
        )
        assert res["status"] == "delivered"
        assert res["http_code"] == 200
        mock_post.assert_called_once()

        call_args = mock_post.call_args
        payload = call_args[1]["json"]

        # Verifica semântica correta: pipeline-ready, não mirror
        assert payload["event"] == "PIPELINE_READY"
        assert payload["dag_id"] == "qimed_master_pipeline_end_to_end"
        assert "dm_glosas_auditoria" in payload["tables_ready"]
        assert "dm_ans_glosas_operadoras" in payload["tables_ready"]

        # Verifica instrução explícita ao backend
        assert payload["olap_source"] == "duckdb_gold"
        assert payload["action_required"] == "invalidate_dashboard_cache"


def test_webhook_default_url_uses_pipeline_ready_endpoint():
    """O endpoint padrão deve apontar para pipeline-ready, não para mirror-trigger."""
    assert "pipeline-ready" in DEFAULT_WEBHOOK_URL
    assert "mirror" not in DEFAULT_WEBHOOK_URL
    assert "mirror-trigger" not in DEFAULT_WEBHOOK_URL


def test_webhook_env_var_override():
    """A variável de ambiente BACKEND_PIPELINE_READY_WEBHOOK_URL substitui o padrão."""
    mock_resp = MagicMock()
    mock_resp.status_code = 202

    custom_url = "http://staging-backend:9000/api/v1/pipeline-ready"
    with patch("requests.post", return_value=mock_resp) as mock_post, \
         patch.dict("os.environ", {"BACKEND_PIPELINE_READY_WEBHOOK_URL": custom_url}):
        res = trigger_sync_webhook(
            dag_id="qimed_gold_aggregation",
            run_id="exec_test_env_456",
            status="success",
        )
        assert res["status"] == "delivered"
        assert res["url"] == custom_url
        mock_post.assert_called_once_with(
            custom_url,
            json=mock_post.call_args[1]["json"],
            headers={"Content-Type": "application/json"},
            timeout=5
        )


def test_webhook_backend_unavailable_graceful_fallback():
    """Tolerância a falhas: backend offline → warning, não bloqueia pipeline."""
    with patch("requests.post", side_effect=Exception("Connection refused")):
        res = trigger_sync_webhook(
            dag_id="qimed_gold_aggregation",
            run_id="exec_test_456",
            status="success",
            ignore_errors=True
        )
        assert res["status"] == "failed_silently"
        assert "Connection refused" in res["error"]


def test_webhook_backend_unavailable_raise_on_error():
    """Com ignore_errors=False, falha de entrega deve propagar exceção."""
    with patch("requests.post", side_effect=Exception("Network timeout")):
        with pytest.raises(Exception, match="Network timeout"):
            trigger_sync_webhook(
                dag_id="qimed_gold_aggregation",
                run_id="exec_test_789",
                status="success",
                ignore_errors=False
            )


def test_webhook_empty_tables_list():
    """tables_ready vazio é válido (ex.: falha parcial de pipeline)."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200

    with patch("requests.post", return_value=mock_resp):
        res = trigger_sync_webhook(
            dag_id="qimed_gold_aggregation",
            run_id="exec_test_empty",
            tables_ready=[],
            status="partial",
            webhook_url="http://test-backend:8000/api/v1/pipeline-ready"
        )
        assert res["status"] == "delivered"
