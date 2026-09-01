"""
Testes automatizados para a camada FastAPI + DuckDB Analytics.
Valida status codes, tipos de retorno, cache, mock do motor DuckDB,
sanitização contra SQL Injection e rota consolidada do Dashboard Financeiro.
"""
import pytest
from fastapi.testclient import TestClient
import duckdb

from src.api.main import app
import src.api.duckdb_query_engine as engine_module
import src.api.cache as cache_module

client = TestClient(app)

SAMPLE_MOCK_DATA = [
    {
        "ano": "2026",
        "mes": "05",
        "uf": "CE",
        "indicador": "teste_kpi",
        "valor": 123.45,
    }
]

SAMPLE_DASHBOARD_MOCK = {
    "kpis": {
        "ticket_medio_brl": 1500.0,
        "mediana_custo_brl": 800.0,
        "custo_total_brl": 50000000.0,
        "custo_medio_obito_brl": 3500.0,
        "custo_medio_alta_brl": 1400.0,
        "razao_custo_obito_alta": 2.5,
        "taxa_glosa_pct": 5.0,
    },
    "top_motivos_glosa_pareto": [
        {
            "codigo_motivo_glosa": "010003",
            "descricao_motivo_glosa": "Glosa Teste",
            "total_glosas": 100,
            "valor_total_glosado_brl": 50000.0,
            "percentual_glosa_pct": 50.0,
            "percentual_acumulado_pareto_pct": 50.0,
        }
    ],
    "custo_por_faixa_permanencia": [
        {
            "faixa_permanencia": "0-3d",
            "total_internacoes": 1000,
            "custo_total_brl": 1000000.0,
            "custo_medio_brl": 1000.0,
            "media_dias_real": 2.0,
        }
    ],
    "serie_temporal_aprovadas_vs_rejeitadas": [
        {
            "periodo": "2026-05",
            "total_aihs_aprovadas": 1000,
            "total_aihs_rejeitadas": 50,
            "valor_aprovado_brl": 1000000.0,
            "valor_glosado_brl": 50000.0,
            "taxa_rejeicao_pct": 4.76,
        }
    ],
}


@pytest.fixture(autouse=True)
def clear_cache():
    """Limpa o cache em memória antes de cada teste."""
    with cache_module._lock:
        cache_module._cache.clear()
    yield
    with cache_module._lock:
        cache_module._cache.clear()


@pytest.fixture
def mock_duckdb(monkeypatch):
    """Fixture que faz monkeypatch em query_gold retornando dados estáticos fixos."""
    def _mock_query(sql: str) -> list[dict]:
        return SAMPLE_MOCK_DATA

    monkeypatch.setattr(engine_module, "query_gold", _mock_query)
    return _mock_query


def test_health_check():
    """Valida endpoint /health de prontidão da API."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["olap_source"] == "duckdb_gold"


def test_get_dashboard_financeiro_real_dw():
    """Valida endpoint consolidado do Dashboard Financeiro no DuckDB DW real."""
    response = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()

    # Valida as 4 estruturas obrigatórias
    assert "kpis" in data
    assert "top_motivos_glosa_pareto" in data
    assert "custo_por_faixa_permanencia" in data
    assert "serie_temporal_aprovadas_vs_rejeitadas" in data

    # Valida campos dos Cards de KPI
    kpis = data["kpis"]
    assert "ticket_medio_brl" in kpis
    assert "mediana_custo_brl" in kpis
    assert "custo_total_brl" in kpis
    assert "razao_custo_obito_alta" in kpis
    assert "taxa_glosa_pct" in kpis

    # Valida listas e tipos
    assert isinstance(data["top_motivos_glosa_pareto"], list)
    assert isinstance(data["custo_por_faixa_permanencia"], list)
    assert isinstance(data["serie_temporal_aprovadas_vs_rejeitadas"], list)


def test_get_dashboard_financeiro_mock(monkeypatch):
    """Valida o endpoint do Dashboard Financeiro com mock isolado."""
    monkeypatch.setattr(
        engine_module,
        "query_dashboard_financeiro",
        lambda periodo, uf="": SAMPLE_DASHBOARD_MOCK,
    )
    response = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05")
    assert response.status_code == 200
    data = response.json()
    assert data == SAMPLE_DASHBOARD_MOCK


def test_get_dashboard_financeiro_sanitization(monkeypatch):
    """Valida resiliência contra SQL Injection no endpoint do Dashboard Financeiro."""
    def _mock_engine_func(periodo: str, uf: str = "") -> dict:
        # Garante que as aspas foram removidas
        assert "'" not in periodo
        assert "'" not in uf
        return SAMPLE_DASHBOARD_MOCK

    monkeypatch.setattr(engine_module, "query_dashboard_financeiro", _mock_engine_func)

    injection_periodo = "2026'; DROP TABLE fct_internacao--"
    injection_uf = "CE' OR '1'='1"
    response = client.get(
        f"/api/v1/analytics/dashboard/financeiro?periodo={injection_periodo}&uf={injection_uf}"
    )
    assert response.status_code == 200


def test_get_glosas_operadoras(mock_duckdb):
    """Valida endpoint /api/v1/analytics/glosas/operadoras."""
    response = client.get("/api/v1/analytics/glosas/operadoras?periodo=2026-05")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["indicador"] == "teste_kpi"


def test_get_glosas_auditoria(mock_duckdb):
    """Valida endpoint /api/v1/analytics/glosas/auditoria."""
    response = client.get("/api/v1/analytics/glosas/auditoria?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1


def test_get_hospitais_eficiencia(mock_duckdb):
    """Valida endpoint /api/v1/analytics/hospitais/eficiencia."""
    response = client.get("/api/v1/analytics/hospitais/eficiencia?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1


def test_get_icsap(mock_duckdb):
    """Valida endpoint /api/v1/analytics/icsap."""
    response = client.get("/api/v1/analytics/icsap?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1


def test_get_anomalias(mock_duckdb):
    """Valida endpoint /api/v1/analytics/anomalias com suporte a paginação."""
    response = client.get("/api/v1/analytics/anomalias?periodo=2026-05")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "paginacao" in data
    assert "itens" in data
    assert len(data["itens"]) >= 0


def test_sql_injection_sanitization(monkeypatch, tmp_path):
    """
    Valida sanitização: tentativa de SQL Injection em periodo
    deve retornar 200 com lista vazia — NÃO deve causar DROP TABLE.
    """
    test_db = str(tmp_path / "test_dw.duckdb")
    with duckdb.connect(test_db) as conn:
        conn.execute("""
            CREATE TABLE dm_ans_glosas_operadoras (
                periodo     VARCHAR,
                taxa_glosa_pct DOUBLE,
                valor_glosado_brl DOUBLE
            );
            INSERT INTO dm_ans_glosas_operadoras VALUES ('2026-05', 5.2, 10000.0);
        """)

    monkeypatch.setattr(
        "src.api.duckdb_query_engine.load_pipeline_config",
        lambda: {"paths": {"gold_dw_file": test_db}},
    )

    injection_payload = "2026'; DROP TABLE dm_ans_glosas_operadoras--"
    response = client.get(f"/api/v1/analytics/glosas/operadoras?periodo={injection_payload}")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data == []  # periodo sanitizado não casa com nenhum registro

    # Confirma que a tabela NÃO foi dropada
    with duckdb.connect(test_db, read_only=True) as conn:
        count = conn.execute("SELECT COUNT(*) FROM dm_ans_glosas_operadoras").fetchone()[0]
        assert count == 1


def test_get_central_anomalias_real_dw():
    """Valida endpoint da Central de Anomalias no DuckDB DW real."""
    response = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&limit=20")
    assert response.status_code == 200
    data = response.json()

    # Validação das 3 chaves principais
    assert "kpis" in data
    assert "paginacao" in data
    assert "anomalias_detectadas" in data

    # Validação dos Cards de KPI
    kpis = data["kpis"]
    assert kpis["anomalias_abertas"] > 0
    assert kpis["valor_em_risco_brl"] > 0
    assert "R$" in kpis["valor_em_risco_formatado"]
    assert kpis["hospitais_afetados_total"] > 0

    # Validação da Paginação
    pag = data["paginacao"]
    assert pag["total_registros"] > 0
    assert pag["pagina_atual"] == 1
    assert pag["limit"] == 20
    assert pag["offset"] == 0

    # Validação dos Itens da Grid
    anomalias = data["anomalias_detectadas"]
    assert isinstance(anomalias, list)
    assert len(anomalias) == 20

    first = anomalias[0]
    assert "id" in first
    assert "prioridade" in first
    assert "tipo" in first
    assert "descricao" in first
    assert "hospital" in first
    assert "impacto_brl" in first
    assert "status" in first


def test_get_central_anomalias_filtros_e_busca():
    """Valida filtros dinâmicos por tipo, prioridade e busca textual na Central de Anomalias."""
    # Filtro por severidade
    r_crit = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&prioridade=CRITICA&limit=10")
    assert r_crit.status_code == 200
    data_crit = r_crit.json()
    for item in data_crit["anomalias_detectadas"]:
        assert item["prioridade"] == "Crítica"

    # Filtro por tipo
    r_outlier = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&tipo=OUTLIER_CUSTO_P99&limit=10")
    assert r_outlier.status_code == 200
    data_out = r_outlier.json()
    for item in data_out["anomalias_detectadas"]:
        assert item["tipo"] == "Outlier de custo"

    # Busca textual por CNES
    r_search = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&search=7042671&limit=5")
    assert r_search.status_code == 200
    data_srch = r_search.json()
    for item in data_srch["anomalias_detectadas"]:
        assert "7042671" in item["codigo_estabelecimento_cnes"] or "7042671" in item["hospital"]


def test_get_central_anomalias_sanitization():
    """Valida que tentativas de injeção SQL nos filtros da Central de Anomalias são neutralizadas."""
    injection = "2026'; DROP TABLE aud_alertas_anomalias--"
    response = client.get(
        f"/api/v1/analytics/central-anomalias?periodo={injection}&search={injection}&tipo={injection}"
    )
    assert response.status_code == 200
    data = response.json()
    assert "kpis" in data
    assert "anomalias_detectadas" in data

