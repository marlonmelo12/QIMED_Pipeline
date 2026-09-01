"""
Suíte de Testes Automatizados para o Painel de Glosas ANS e Detecção Estatística de Outliers.
"""
import pytest
from fastapi.testclient import TestClient
from src.api.main import app
from src.analytics.outliers import detectar_outliers_mad

client = TestClient(app)


# ==============================================================================
# 1. TESTES UNITÁRIOS DA METODOLOGIA ESTATÍSTICA DE OUTLIERS (MAD)
# ==============================================================================

def test_outlier_cenario_a_sem_outlier():
    """Cenário A: Distribuição homogênea sem nenhum outlier estatístico."""
    dados_normais = [
        {"codigo_registro_ans": f"00000{i}", "razao_social": f"Operadora {i}", "valor_total_glosado_brl": 1000.0 + (i * 20)}
        for i in range(15)
    ]
    res = detectar_outliers_mad(dados_normais, threshold_mad=3.5, threshold_concentracao_pct=50.0)

    assert res.has_outlier is False
    assert res.outliers_count == 0
    assert res.operadora_outlier_principal is None
    assert res.codigo_outlier_principal is None
    assert res.concentracao_pct_principal is None
    assert len(res.outliers_detectados) == 0
    assert "Nenhuma concentração" in res.mensagem or "Nenhuma" in res.mensagem


def test_outlier_cenario_b_um_outlier_dominante():
    """Cenário B: Exatamente um outlier extremo com dominância de concentração."""
    dados = [
        {"codigo_registro_ans": f"00000{i}", "razao_social": f"Operadora {i}", "valor_total_glosado_brl": 1000.0}
        for i in range(10)
    ]
    dados.append({
        "codigo_registro_ans": "999999",
        "razao_social": "OPERADORA ATÍPICA S.A.",
        "valor_total_glosado_brl": 1000000.0,
    })

    res = detectar_outliers_mad(dados, threshold_mad=3.5, threshold_concentracao_pct=50.0)

    assert res.has_outlier is True
    assert res.outliers_count == 1
    assert res.codigo_outlier_principal == "999999"
    assert res.operadora_outlier_principal == "OPERADORA ATÍPICA S.A."
    assert res.concentracao_pct_principal is not None
    assert res.concentracao_pct_principal > 90.0
    assert len(res.outliers_detectados) == 1


def test_outlier_cenario_c_multiplos_outliers():
    """Cenário C: Múltiplos candidatos a outlier estatístico (retorna lista e destaca o principal)."""
    dados = [
        {"codigo_registro_ans": f"00000{i}", "razao_social": f"Operadora {i}", "valor_total_glosado_brl": 1000.0}
        for i in range(20)
    ]
    dados.append({
        "codigo_registro_ans": "888888",
        "razao_social": "OUTLIER SECUNDÁRIO",
        "valor_total_glosado_brl": 400000.0,
    })
    dados.append({
        "codigo_registro_ans": "999999",
        "razao_social": "OUTLIER PRIMÁRIO",
        "valor_total_glosado_brl": 900000.0,
    })

    res = detectar_outliers_mad(dados, threshold_mad=3.5, threshold_concentracao_pct=50.0)

    assert res.has_outlier is True
    assert res.outliers_count >= 2
    assert res.codigo_outlier_principal == "999999"
    assert res.operadora_outlier_principal == "OUTLIER PRIMÁRIO"
    assert len(res.outliers_detectados) >= 2
    # Verifica ordenação decrescente por valor
    assert res.outliers_detectados[0].valor >= res.outliers_detectados[1].valor


# ==============================================================================
# 2. TESTES DE CONTRATO DA API E REQUISITOS DE INTEGRIDADE
# ==============================================================================

def test_painel_glosa_ans_visao_setor():
    """Valida o endpoint do Painel de Glosa ANS na visão Setor (com expurgo de outlier)."""
    res = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025&visao=setor")
    assert res.status_code == 200
    data = res.json()

    # 1. Estrutura Principal
    assert "visao_aplicada" in data
    assert data["visao_aplicada"] == "setor"
    assert "kpis" in data
    assert "alerta_anomalia_outlier" in data
    assert "detalhamento_glosa_inicial" in data

    # 2. 5 Cards de KPI (Valores Numéricos Puros)
    kpis = data["kpis"]
    assert "tempo_medio_pagamento_dias" in kpis
    assert "taxa_glosa_inicial_pct" in kpis
    assert "taxa_glosa_final_pct" in kpis
    assert "recuperacao_pp" in kpis
    assert "pct_guias_sem_retorno_60d" in kpis
    assert "pct_valor_sem_retorno_60d" in kpis

    assert isinstance(kpis["taxa_glosa_inicial_pct"], (int, float))
    assert isinstance(kpis["tempo_medio_pagamento_dias"], (int, float))

    # 3. Alerta de Outlier
    outlier = data["alerta_anomalia_outlier"]
    assert "has_outlier" in outlier
    assert "metodologia" in outlier
    assert outlier["metodologia"] == "modified_z_score_mad"

    # 4. Detalhamento (3 dimensões)
    det = data["detalhamento_glosa_inicial"]
    assert "por_porte" in det
    assert "por_segmentacao" in det
    assert "por_modalidade" in det
    assert len(det["por_porte"]) > 0
    assert len(det["por_segmentacao"]) > 0
    assert len(det["por_modalidade"]) > 0

    p0 = det["por_porte"][0]
    assert "porte" in p0
    assert "taxa_glosa_inicial_pct" in p0
    assert "total_faturado_brl" in p0


def test_painel_glosa_ans_visao_operadora_paginada():
    """Valida a visão Operadora com paginação estrita e listagem de entidades."""
    res = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025&visao=operadora&limit=5&offset=0")
    assert res.status_code == 200
    data = res.json()

    assert data["visao_aplicada"] == "operadora"
    assert "paginacao" in data
    assert "operadoras" in data

    pag = data["paginacao"]
    assert pag["limit"] == 5
    assert pag["offset"] == 0
    assert pag["total_registros"] >= 1
    assert pag["pagina_atual"] == 1
    assert pag["total_paginas"] >= 1

    ops = data["operadoras"]
    assert len(ops) <= 5
    if ops:
        op0 = ops[0]
        assert "codigo_registro_ans" in op0
        assert "razao_social" in op0
        assert "valor_total_glosado_brl" in op0
        assert "taxa_glosa_pct" in op0


def test_painel_glosa_ans_filtros_multidimensionais():
    """Valida a aplicação de filtros de porte, segmentação e modalidade."""
    # Filtro por Segmentação
    res_seg = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025&segmentacao=Exclus.+Odontol%C3%B3gica")
    assert res_seg.status_code == 200
    data_seg = res_seg.json()
    for item in data_seg["detalhamento_glosa_inicial"]["por_segmentacao"]:
        assert "Odonto" in item["segmentacao"]

    # Filtro por Porte
    res_porte = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025&porte=Grande")
    assert res_porte.status_code == 200


def test_alias_glosas_operadoras_compatibilidade():
    """Valida o alias de compatibilidade /analytics/glosas/operadoras."""
    res = client.get("/api/v1/analytics/glosas/operadoras?periodo=2025&limit=10&offset=0")
    assert res.status_code == 200
    data = res.json()

    assert data["visao_aplicada"] == "operadora"
    assert "paginacao" in data
    assert "operadoras" in data
    assert len(data["operadoras"]) <= 10
