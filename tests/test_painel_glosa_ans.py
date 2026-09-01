import time
import pytest
from fastapi.testclient import TestClient
from src.api.main import app
import duckdb
from src.api.duckdb_query_engine import query_painel_glosa_ans

client = TestClient(app)


def test_painel_glosa_ans_status_200_and_latency():
    """
    Critério de Aceitação:
    A rota GET /api/v1/analytics/painel-glosa-ans?periodo=2025 responde com Status 200 OK em menos de 15ms.
    """
    # Warmup
    client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025")

    t0 = time.perf_counter()
    response = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2025")
    elapsed_ms = (time.perf_counter() - t0) * 1000

    assert response.status_code == 200
    assert elapsed_ms < 15.0, f"Latência de resposta excedeu 15ms: {elapsed_ms:.2f}ms"


def test_painel_glosa_ans_structure():
    """
    Valida que o payload JSON contém as 3 estruturas principais:
    1. kpis (5 cards);
    2. alerta_anomalia_outlier;
    3. detalhamento_glosa_inicial (3 dimensões).
    """
    response = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05")
    assert response.status_code == 200
    data = response.json()

    # 1. Os 5 Cards Superiores de KPI
    assert "kpis" in data
    kpis = data["kpis"]
    assert "tempo_medio_pagamento_dias" in kpis
    assert "taxa_glosa_inicial_pct" in kpis
    assert "taxa_glosa_final_pct" in kpis
    assert "pct_guias_sem_retorno_60d" in kpis
    assert "pct_valor_sem_retorno_60d" in kpis

    assert isinstance(kpis["tempo_medio_pagamento_dias"], (int, float))
    assert isinstance(kpis["taxa_glosa_inicial_pct"], (int, float))
    assert isinstance(kpis["taxa_glosa_final_pct"], (int, float))
    assert isinstance(kpis["pct_guias_sem_retorno_60d"], (int, float))
    assert isinstance(kpis["pct_valor_sem_retorno_60d"], (int, float))

    # 2. Detector de Operadora Atípica (Outlier)
    assert "alerta_anomalia_outlier" in data
    outlier = data["alerta_anomalia_outlier"]
    assert "has_outlier" in outlier
    assert "operadora_outlier" in outlier
    assert "concentracao_pct" in outlier
    assert "mensagem" in outlier
    assert isinstance(outlier["has_outlier"], bool)
    assert isinstance(outlier["concentracao_pct"], (int, float))
    assert isinstance(outlier["mensagem"], str)

    # 3. Detalhamento Multidimensional
    assert "detalhamento_glosa_inicial" in data
    detalhes = data["detalhamento_glosa_inicial"]
    assert "por_porte" in detalhes
    assert "por_segmentacao" in detalhes
    assert "por_modalidade" in detalhes

    assert isinstance(detalhes["por_porte"], list)
    assert isinstance(detalhes["por_segmentacao"], list)
    assert isinstance(detalhes["por_modalidade"], list)


def test_painel_glosa_ans_dynamic_filters():
    """
    Testa suporte aos filtros dinâmicos de segmentação, modalidade, porte e registro_ans.
    """
    res_mod = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05&modalidade=Cooperativa%20M%C3%A9dica")
    assert res_mod.status_code == 200

    res_seg = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05&segmentacao=M%C3%A9dico-Hospitalar")
    assert res_seg.status_code == 200

    res_porte = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05&porte=Grande")
    assert res_porte.status_code == 200

    res_op = client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05&visao=operadora&registro_ans=363855")
    assert res_op.status_code == 200


def test_outlier_detection_logic(monkeypatch, tmp_path):
    test_db = str(tmp_path / "test_outlier.duckdb")
    with duckdb.connect(test_db) as conn:
        conn.execute("""
        CREATE TABLE dm_ans_glosas_operadoras (
            id_registro_kpi VARCHAR,
            codigo_registro_ans VARCHAR,
            cnpj_operadora VARCHAR,
            razao_social VARCHAR,
            modalidade_operadora VARCHAR,
            ano VARCHAR,
            mes VARCHAR,
            periodo VARCHAR,
            total_guias_glosadas BIGINT,
            valor_total_faturado_brl DOUBLE,
            valor_total_recolhido_brl DOUBLE,
            valor_total_glosado_brl DOUBLE,
            taxa_glosa_pct DOUBLE
        );
        INSERT INTO dm_ans_glosas_operadoras VALUES
        ('1', '999999', '00000000000100', 'OPERADORA MONOPOLISTA OUTLIER', 'Autogestão', '2026', '05', '2026-05', 950, 1000000.0, 50000.0, 950000.0, 95.0),
        ('2', '111111', '00000000000101', 'OPERADORA NORMAL A', 'Cooperativa Médica', '2026', '05', '2026-05', 20, 100000.0, 80000.0, 20000.0, 20.0),
        ('3', '222222', '00000000000102', 'OPERADORA NORMAL B', 'Medicina de Grupo', '2026', '05', '2026-05', 30, 100000.0, 70000.0, 30000.0, 30.0);
        """)

    monkeypatch.setattr("src.api.duckdb_query_engine.load_pipeline_config", lambda: {"paths": {"gold_dw_file": test_db}})

    # 1. Visão Setorial com Expurgo
    res_setor = query_painel_glosa_ans(periodo="2026-05", visao="setor")
    assert res_setor["alerta_anomalia_outlier"]["has_outlier"] is True
    assert res_setor["alerta_anomalia_outlier"]["operadora_outlier"] == "OPERADORA MONOPOLISTA OUTLIER"
    assert res_setor["alerta_anomalia_outlier"]["concentracao_pct"] == 95.0

    # 2. Visão Operadora
    res_operadora = query_painel_glosa_ans(periodo="2026-05", visao="operadora")
    assert res_operadora["kpis"]["taxa_glosa_inicial_pct"] == 83.33


def test_sql_injection_defense():
    malicious_period = "2025' OR '1'='1"
    response = client.get(f"/api/v1/analytics/painel-glosa-ans?periodo={malicious_period}")
    assert response.status_code == 200
