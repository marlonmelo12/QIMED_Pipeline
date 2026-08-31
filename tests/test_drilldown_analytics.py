"""
Testes automatizados para a suíte de endpoints de Drill-Down Analytics.
Valida status codes, estruturas JSON (Padrão BFF), curva de Gompertz-Makeham,
decomposição SH/SP, percentis de ticket médio e sanitização contra SQL Injection.
"""
import pytest
from fastapi.testclient import TestClient
from src.api.main import app
import src.api.duckdb_query_engine as engine_module
import src.api.cache as cache_module

client = TestClient(app)


@pytest.fixture(autouse=True)
def clear_cache():
    """Limpa o cache em memória antes de cada teste."""
    with cache_module._lock:
        cache_module._cache.clear()
    yield
    with cache_module._lock:
        cache_module._cache.clear()


def test_drilldown_ticket_medio_real_dw():
    """Valida endpoint de Drill-Down de Ticket Médio no DuckDB DW real."""
    response = client.get("/api/v1/analytics/drilldown/ticket-medio?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()

    # Validação do Schema
    assert "kpi_resumo" in data
    assert "distribuicao_percentis" in data
    assert "evolucao_mensal" in data
    assert "ranking_hospitais_cnes" in data
    assert "quebra_por_uf" in data

    # Validação dos KPIs
    kpi = data["kpi_resumo"]
    assert kpi["ticket_medio_brl"] > 0
    assert kpi["mediana_custo_brl"] > 0
    assert kpi["p75_custo_brl"] >= kpi["mediana_custo_brl"]
    assert kpi["total_internacoes"] > 0

    # Validação da curva de percentis
    perc = data["distribuicao_percentis"]
    assert perc["p25"] <= perc["p50"] <= perc["p75"] <= perc["p90"] <= perc["p99"]

    # Validação das listas
    assert isinstance(data["evolucao_mensal"], list)
    assert isinstance(data["ranking_hospitais_cnes"], list)
    assert len(data["ranking_hospitais_cnes"]) > 0


def test_drilldown_custo_total_real_dw():
    """Valida endpoint de Drill-Down de Custo Total no DuckDB DW real."""
    response = client.get("/api/v1/analytics/drilldown/custo-total?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()

    # Validação do Schema
    assert "kpi_resumo" in data
    assert "decomposicao_custos" in data
    assert "evolucao_mensal" in data
    assert "ranking_hospitais_cnes" in data
    assert "quebra_por_uf" in data

    # Validação da decomposição SH/SP/UTI
    kpi = data["kpi_resumo"]
    assert kpi["custo_total_brl"] > 0
    assert kpi["custo_sh_brl"] > 0
    assert kpi["custo_sp_brl"] > 0
    assert kpi["percentual_sh_pct"] > 0
    assert kpi["percentual_sp_pct"] > 0

    # Validação da lista de decomposição
    decom = data["decomposicao_custos"]
    assert isinstance(decom, list)
    assert len(decom) == 3
    componentes = [d["componente"] for d in decom]
    assert any("Serviços Hospitalares" in c for c in componentes)
    assert any("Serviços Profissionais" in c for c in componentes)


def test_drilldown_custo_desfecho_real_dw():
    """Valida endpoint de Drill-Down de Custo por Desfecho (Óbito vs Alta) e Curva de Gompertz."""
    response = client.get("/api/v1/analytics/drilldown/custo-desfecho?periodo=2026-05&uf=CE")
    assert response.status_code == 200
    data = response.json()

    # Validação do Schema
    assert "kpi_resumo" in data
    assert "curva_mortalidade_custo_por_idade" in data
    assert "evolucao_mensal" in data
    assert "ranking_hospitais_mortalidade" in data

    # Validação dos KPIs comparativos
    kpi = data["kpi_resumo"]
    assert kpi["total_internacoes"] > 0
    assert kpi["total_obitos"] > 0
    assert kpi["total_altas"] > 0
    assert kpi["custo_medio_obito_brl"] > 0
    assert kpi["custo_medio_alta_brl"] > 0
    assert kpi["razao_custo_obito_alta"] > 1.0  # Custo do óbito historicamente é superior

    # Validação da Curva de Gompertz-Makeham
    curva = data["curva_mortalidade_custo_por_idade"]
    assert isinstance(curva, list)
    assert len(curva) >= 5
    faixas = [f["faixa_etaria"] for f in curva]
    assert "0-1 ano" in faixas
    assert "80+ anos" in faixas


def test_drilldown_sql_injection_sanitization():
    """Valida que tentativas de injeção SQL em periodo e uf são neutralizadas."""
    injection_periodo = "2026'; DROP TABLE fct_internacao--"
    injection_uf = "CE' OR 1=1--"

    rotas = [
        f"/api/v1/analytics/drilldown/ticket-medio?periodo={injection_periodo}&uf={injection_uf}",
        f"/api/v1/analytics/drilldown/custo-total?periodo={injection_periodo}&uf={injection_uf}",
        f"/api/v1/analytics/drilldown/custo-desfecho?periodo={injection_periodo}&uf={injection_uf}",
    ]

    for rota in rotas:
        response = client.get(rota)
        assert response.status_code == 200
        data = response.json()
        assert "kpi_resumo" in data
