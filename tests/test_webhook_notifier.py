"""
Testes Unitários do Módulo de Notificação Reativa via Webhook (Mirror Trigger).
"""
import pytest
from unittest.mock import patch, MagicMock
from src.observability.webhook_notifier import trigger_sync_webhook, DEFAULT_WEBHOOK_URL


def test_webhook_successful_delivery():
    """Testa entrega de notificação com status 200 OK do backend."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200

    with patch("requests.post", return_value=mock_resp) as mock_post:
        res = trigger_sync_webhook(
            dag_id="qimed_master_pipeline_end_to_end",
            run_id="exec_test_123",
            tables_ready=["dm_ans_glosas_operadoras"],
            status="success",
            webhook_url="http://test-backend:8000/api/v1/sync/mirror-trigger"
        )
        assert res["status"] == "delivered"
        assert res["http_code"] == 200
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["event"] == "PIPELINE_COMPLETED"
        assert call_args[1]["json"]["dag_id"] == "qimed_master_pipeline_end_to_end"
        assert call_args[1]["json"]["tables_ready"] == ["dm_ans_glosas_operadoras"]


def test_webhook_backend_unavailable_graceful_fallback():
    """Testa tolerância a falhas quando o backend está offline (ignore_errors=True)."""
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
    """Testa raise explícito quando ignore_errors=False."""
    with patch("requests.post", side_effect=Exception("Network timeout")):
        with pytest.raises(Exception, match="Network timeout"):
            trigger_sync_webhook(
                dag_id="qimed_gold_aggregation",
                run_id="exec_test_789",
                status="success",
                ignore_errors=False
            )
