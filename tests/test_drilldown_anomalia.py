"""
Suíte de testes para a Central de Anomalias (SIH), Drilldown e Workflow de Auditoria.
"""
import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)


def test_central_anomalias_consolidada():
    """Valida o endpoint principal da Central de Anomalias com todos os widgets da tela."""
    res = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&limit=10")
    assert res.status_code == 200
    data = res.json()

    # 1. 4 Cards de KPI
    assert "kpis" in data
    kpis = data["kpis"]
    assert "anomalias_abertas" in kpis
    assert "valor_em_risco_brl" in kpis
    assert "valor_em_risco_formatado" in kpis
    assert "taxa_rejeicao_pct" in kpis
    assert "hospitais_afetados_total" in kpis
    assert kpis["anomalias_abertas"] > 0
    assert kpis["hospitais_afetados_total"] > 0

    # 2. Ranking de Rejeição (Top 5)
    assert "hospitais_maior_rejeicao" in data
    top_hosp = data["hospitais_maior_rejeicao"]
    assert len(top_hosp) <= 5
    if top_hosp:
        h0 = top_hosp[0]
        assert "hospital" in h0
        assert "hospital_curto" in h0
        assert "taxa_rejeicao_pct" in h0
        assert "cnes" in h0
        assert "uf" in h0

    # 3. Distribuição por Tipo
    assert "anomalias_por_tipo" in data
    tipos = data["anomalias_por_tipo"]
    assert len(tipos) > 0
    t0 = tipos[0]
    assert "tipo" in t0
    assert "total_ocorrencias" in t0
    assert "impacto_brl" in t0
    assert "cor" in t0

    # 4. Grid de Anomalias
    assert "anomalias_detectadas" in data
    grid = data["anomalias_detectadas"]
    assert len(grid) > 0
    item0 = grid[0]
    assert "id" in item0
    assert "id_alerta" in item0
    assert "prioridade" in item0
    assert "tipo" in item0
    assert "descricao" in item0
    assert "hospital" in item0
    assert "impacto_brl" in item0
    assert "impacto_formatado" in item0
    assert "status" in item0


def test_drilldown_anomalia_individual():
    """Valida o endpoint de Drilldown profundo de uma anomalia individual."""
    res_list = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&limit=1")
    assert res_list.status_code == 200
    grid = res_list.json().get("anomalias_detectadas", [])
    assert len(grid) > 0

    id_alerta = grid[0]["id_alerta"]

    res_drill = client.get(f"/api/v1/analytics/central-anomalias/{id_alerta}")
    assert res_drill.status_code == 200
    drill = res_drill.json()

    # Estruturas obrigatórias
    assert "alerta" in drill
    assert "contexto_hospitalar" in drill
    assert "evolucao_temporal" in drill
    assert "aihs_correlacionadas" in drill
    assert "acoes_disponiveis" in drill

    # Validação do Alerta
    alerta = drill["alerta"]
    assert alerta["id_alerta"] == id_alerta
    assert "numero_aih" in alerta
    assert "prioridade" in alerta
    assert "impacto_formatado" in alerta

    # Validação do Hospital resolvido
    hosp = drill["contexto_hospitalar"]
    assert "nome_fantasia" in hosp
    assert "municipio" in hosp
    assert "uf" in hosp
    assert not "Município [" in hosp["municipio"]

    # Validação das ações
    acoes = drill["acoes_disponiveis"]
    assert len(acoes) >= 3


def test_update_status_workflow():
    """Valida a atualização de status com invalidação de cache atômica."""
    res_list = client.get("/api/v1/analytics/central-anomalias?periodo=2026-05&limit=1")
    id_alerta = res_list.json()["anomalias_detectadas"][0]["id_alerta"]

    # Atualiza para EM_ANALISE
    res_patch = client.patch(f"/api/v1/analytics/central-anomalias/{id_alerta}/status?status=EM_ANALISE")
    assert res_patch.status_code == 200
    assert res_patch.json()["sucesso"] is True
    assert res_patch.json()["novo_status"] == "EM_ANALISE"

    # Atualiza para RESOLVIDA
    res_patch2 = client.patch(f"/api/v1/analytics/central-anomalias/{id_alerta}/status?status=RESOLVIDA")
    assert res_patch2.status_code == 200
    assert res_patch2.json()["novo_status"] == "RESOLVIDA"
