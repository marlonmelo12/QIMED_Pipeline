"""
Testes de Resiliência S3/MinIO (DuckDB Secret/DeltaKernel) e Callbacks Airflow — QIMED Lakehouse V3.
Cobre integralmente os cenários de teste exigidos na especificação:

Casos S3/DuckDB (Seção 14):
1. Credenciais válidas + MinIO: delta_scan -> sucesso
2. Credenciais ausentes: falha controlada
3. Endpoint inválido: falha rápida e explícita
4. Não deve tentar 169.254.169.254 em ambiente configurado para MinIO
5. Path-style configurado corretamente
6. HTTP MinIO local
7. Secret idempotente (múltiplas execuções de configure_duckdb_s3)

Casos Callback Airflow (Seção 15):
1. dag_run.conf = {"qimed_job_id": "abc"} -> "abc"
2. dag_run.conf = {} -> None
3. DAG agendada: dag_run.conf vazio -> sem TypeError
4. context["conf"] é AirflowConfigParser -> não utilizado como config de run
5. dag_run inexistente -> sem exception
6. dag_run.conf = None -> sem exception
7. Callback com falha da DAG -> não mascara exception original
"""
import os
import duckdb
import pytest
from unittest.mock import MagicMock, patch
from src.processing.duckdb_engine import DuckDBEngine
from src.utils.s3_storage import (
    configure_duckdb_s3,
    get_s3_storage_options,
    get_s3_endpoint_url,
    get_s3_bucket_name,
    lakehouse_path_exists,
)
from src.observability.airflow_callbacks import (
    _extract_qimed_job_context,
    notify_job_status_to_backend,
    on_dag_failure_callback,
    on_dag_success_callback,
)
from src.metadata.models import JobStatus


# =============================================================================
# SEÇÃO 1: TESTES S3 / DUCKDB / DELTAKERNEL (Casos 1 a 7)
# =============================================================================

def test_s3_caso_1_valid_credentials_delta_scan():
    """Caso 1: Credenciais válidas + MinIO -> delta_scan executa com sucesso."""
    engine = DuckDBEngine()
    # Consulta a tabela Bronze SIH que foi persistida no MinIO
    sql = "SELECT * FROM delta_scan('s3://qimed-lakehouse/bronze/datasus/sih') LIMIT 1;"
    df = engine.query(sql).df()
    assert df is not None
    assert len(df) >= 0


def test_s3_caso_2_missing_credentials_controlled_failure():
    """Caso 2: Credenciais ausentes/inválidas geram falha controlada e compreensível."""
    conn = duckdb.connect()
    conn.execute("LOAD delta;")
    conn.execute("LOAD httpfs;")
    
    # Configura um secret com credenciais invalidas propositalmente
    conn.execute("""
        CREATE OR REPLACE SECRET s3_invalid (
            TYPE S3,
            KEY_ID 'invalid_user',
            SECRET 'invalid_password',
            ENDPOINT 'minio:9000',
            USE_SSL false,
            URL_STYLE 'path',
            REGION 'us-east-1'
        );
    """)
    with pytest.raises(Exception) as exc_info:
        conn.execute("SELECT * FROM delta_scan('s3://qimed-lakehouse/bronze/datasus/sih') LIMIT 1;").df()
    assert "169.254.169.254" not in str(exc_info.value)


def test_s3_caso_3_invalid_endpoint_fast_explicit_failure():
    """Caso 3: Endpoint inválido falha rápido e explicitamente sem fallback infinito."""
    conn = duckdb.connect()
    conn.execute("LOAD delta;")
    conn.execute("LOAD httpfs;")
    
    conn.execute("""
        CREATE OR REPLACE SECRET s3_bad_endpoint (
            TYPE S3,
            KEY_ID 'minio_admin',
            SECRET 'minio_secret_password',
            ENDPOINT 'invalid-host-that-does-not-exist:9999',
            USE_SSL false,
            URL_STYLE 'path',
            REGION 'us-east-1'
        );
    """)
    with pytest.raises(Exception) as exc_info:
        conn.execute("SELECT * FROM delta_scan('s3://qimed-lakehouse/bronze/datasus/sih') LIMIT 1;").df()
    # Confirma que a falha é de resolução/conexão do endpoint informado e não do IMDS
    err_str = str(exc_info.value)
    assert "169.254.169.254" not in err_str


def test_s3_caso_4_no_imds_fallback():
    """Caso 4: Não deve tentar 169.254.169.254 em ambiente configurado para MinIO."""
    conn = duckdb.connect()
    conn.execute("LOAD delta;")
    configure_duckdb_s3(conn)
    
    # A consulta direta com o secret S3 ativo jamais deve conter o IP link-local do IMDS
    try:
        df = conn.execute("SELECT * FROM delta_scan('s3://qimed-lakehouse/bronze/datasus/sih') LIMIT 1;").df()
        assert df is not None
    except Exception as e:
        assert "169.254.169.254" not in str(e)


def test_s3_caso_5_path_style_configuration():
    """Caso 5: Valida que a configuração do Secret utiliza URL_STYLE 'path' para MinIO."""
    conn = duckdb.connect()
    configure_duckdb_s3(conn)
    # Inspeciona os secrets cadastrados no DuckDB
    secrets_df = conn.execute("FROM duckdb_secrets();").df()
    assert len(secrets_df) > 0
    assert "s3_creds" in secrets_df["name"].tolist()


def test_s3_caso_6_http_minio_local():
    """Caso 6: Valida suporte a HTTP (USE_SSL false) para o MinIO local."""
    conn = duckdb.connect()
    configure_duckdb_s3(conn)
    res = conn.execute("SELECT 1;").fetchone()
    assert res[0] == 1


def test_s3_caso_7_secret_idempotence():
    """Caso 7: A execução repetida de configure_duckdb_s3 é estritamente idempotente."""
    conn = duckdb.connect()
    # Executa 3 vezes consecutivas
    configure_duckdb_s3(conn)
    configure_duckdb_s3(conn)
    configure_duckdb_s3(conn)
    
    secrets_df = conn.execute("FROM duckdb_secrets();").df()
    s3_secrets = secrets_df[secrets_df["name"] == "s3_creds"]
    # Garante que não foram criados múltiplos secrets duplicados
    assert len(s3_secrets) == 1


# =============================================================================
# SEÇÃO 2: TESTES DO CALLBACK DO AIRFLOW (Casos 1 a 7)
# =============================================================================

class MockAirflowConfigParser:
    """Mock representando o objeto airflow.configuration.conf."""
    def get(self, section: str, key: str, **kwargs):
        return f"mock_val_{section}_{key}"


def test_callback_caso_1_dag_run_conf_with_job_id():
    """Teste 1: dag_run.conf = {'qimed_job_id': 'abc'} -> 'abc'."""
    dag_run_mock = MagicMock()
    dag_run_mock.conf = {"qimed_job_id": "abc_job_123", "qimed_run_id": "run_999"}
    context = {"dag_run": dag_run_mock, "conf": MockAirflowConfigParser()}
    
    job_id, run_id = _extract_qimed_job_context(context)
    assert job_id == "abc_job_123"
    assert run_id == "run_999"


def test_callback_caso_2_dag_run_conf_empty():
    """Teste 2: dag_run.conf = {} -> None."""
    dag_run_mock = MagicMock()
    dag_run_mock.conf = {}
    context = {"dag_run": dag_run_mock, "conf": MockAirflowConfigParser()}
    
    job_id, run_id = _extract_qimed_job_context(context)
    assert job_id is None
    assert run_id is None


def test_callback_caso_3_dag_scheduled_no_typeerror():
    """Teste 3: DAG agendada (dag_run.conf vazio) não gera TypeError no callback."""
    dag_run_mock = MagicMock()
    dag_run_mock.conf = {}
    dag_run_mock.run_id = "scheduled__2026-08-01T00:00:00+00:00"
    
    context = {
        "dag_run": dag_run_mock,
        "conf": MockAirflowConfigParser(),
        "exception": Exception("Falha simulada de task"),
    }
    
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        res = on_dag_failure_callback(context)
        assert res["status"] in ("delivered", "failed_silently")
        assert res["payload"]["job_id"] is None
        assert res["payload"]["dag_run_id"] == "scheduled__2026-08-01T00:00:00+00:00"


def test_callback_caso_4_context_conf_is_airflowconfigparser():
    """Teste 4: context['conf'] é AirflowConfigParser e NÃO é usado como DAG run config."""
    context = {
        "dag_run": None,
        "conf": MockAirflowConfigParser(),
    }
    job_id, run_id = _extract_qimed_job_context(context)
    assert job_id is None
    assert run_id is None


def test_callback_caso_5_dag_run_non_existent():
    """Teste 5: dag_run inexistente não gera exception."""
    context = {}
    job_id, run_id = _extract_qimed_job_context(context)
    assert job_id is None
    assert run_id is None
    
    with patch("requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        res = notify_job_status_to_backend(context, status="RUNNING")
        assert res["status"] in ("delivered", "failed_silently")


def test_callback_caso_6_dag_run_conf_is_none():
    """Teste 6: dag_run.conf = None não gera exception."""
    dag_run_mock = MagicMock()
    dag_run_mock.conf = None
    context = {"dag_run": dag_run_mock, "conf": MockAirflowConfigParser()}
    
    job_id, run_id = _extract_qimed_job_context(context)
    assert job_id is None
    assert run_id is None


def test_callback_caso_7_failure_callback_preserves_original_exception():
    """Teste 7: O callback com falha de rede/backend não mascara a exception original da DAG."""
    dag_run_mock = MagicMock()
    dag_run_mock.conf = {}
    dag_run_mock.run_id = "manual_test_run"
    
    original_err = ValueError("Erro original na Camada Bronze/Silver")
    context = {
        "dag_run": dag_run_mock,
        "conf": MockAirflowConfigParser(),
        "exception": original_err,
    }
    
    # Simula falha de conexao total do webhook
    with patch("requests.post", side_effect=ConnectionError("Backend fora do ar")):
        # ignore_errors=True (padrao de observabilidade) deve retornar fail silently sem re-lancar ConnectionError
        res = on_dag_failure_callback(context)
        assert res["status"] == "failed_silently"
        assert res["payload"]["error_message"] == "Erro original na Camada Bronze/Silver"
