"""
Testes Automatizados da Central de Anomalias e Auditoria Hospitalar (Task 13).
"""
import duckdb
import pytest
import pandas as pd
import numpy as np
from src.gold.models.kpi_central_anomalias import get_central_anomalias_sql, build_aud_alertas_anomalias


def test_regra1_outlier_custo_p99_e_excesso_custo():
    """Valida detec??o de outliers > P99 e c?lculo exato de excesso de custo sobre o P90 [Task 13]."""
    conn = duckdb.connect()
    
    # Cria uma distribui??o com 100 interna??es para o procedimento '0408050160'
    valores = [1000.0 + i * 10.0 for i in range(98)] + [5000.0, 15000.0]
    df_raw = pd.DataFrame([
        {
            "numero_aih": f"123456789{i:04d}",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0408050160",
            "tipo_identificacao_aih": "1",
            "indicador_obito": False,
            "dias_permanencia_real": 5,
            "valor_total_brl": val,
        }
        for i, val in enumerate(valores)
    ])
    
    # Executa a query da Central de Anomalias
    sql = get_central_anomalias_sql(fct_internacao_source="df_raw", min_proc_count=5)
    df_res = conn.execute(sql).df()
    conn.close()

    outliers = df_res[df_res["tipo_anomalia"] == "OUTLIER_CUSTO_P99"]
    assert len(outliers) > 0, "Deveria disparar alerta para AIH com custo > P99."
    
    # Verifica que o maior custo (15000.0) foi capturado
    maior_outlier = outliers[outliers["valor_faturado_brl"] == 15000.0].iloc[0]
    assert maior_outlier["severidade"] == "ALTA"
    assert maior_outlier["excesso_custo_brl"] > 0
    assert maior_outlier["excesso_custo_brl"] == maior_outlier["valor_faturado_brl"] - maior_outlier["custo_esperado_brl"]
    assert (outliers["excesso_custo_brl"] >= 0.0).all(), "Excesso de custo nunca pode ser negativo."


def test_regra2_aih_inicial_valor_zero():
    """Valida detec??o de AIHs iniciais faturadas com valor zero [Task 13]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {
            "numero_aih": "1111111111111",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0301010072",
            "tipo_identificacao_aih": "1",
            "indicador_obito": False,
            "dias_permanencia_real": 3,
            "valor_total_brl": 0.0,
        },
        {
            "numero_aih": "2222222222222",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0301010072",
            "tipo_identificacao_aih": "5",  # AIH de continua??o com 0 n?o ? anomalia da regra 2
            "indicador_obito": False,
            "dias_permanencia_real": 30,
            "valor_total_brl": 0.0,
        },
    ])
    
    sql = get_central_anomalias_sql(fct_internacao_source="df_raw", min_proc_count=1)
    df_res = conn.execute(sql).df()
    conn.close()

    val_zero = df_res[df_res["tipo_anomalia"] == "AIH_VALOR_ZERO"]
    assert len(val_zero) == 1, "Apenas a AIH inicial (tipo 1) com valor 0 deve disparar alerta AIH_VALOR_ZERO."
    assert val_zero.iloc[0]["numero_aih"] == "1111111111111"
    assert val_zero.iloc[0]["severidade"] == "MEDIA"
    assert val_zero.iloc[0]["status_operacional"] == "NOVA"


def test_regra3_obito_permanencia_zero():
    """Valida detec??o de ?bitos imediatos (no 1? dia de admiss?o) [Task 13]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {
            "numero_aih": "3333333333333",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0303010037",
            "tipo_identificacao_aih": "1",
            "indicador_obito": True,
            "dias_permanencia_real": 0,
            "valor_total_brl": 850.0,
        },
        {
            "numero_aih": "4444444444444",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0303010037",
            "tipo_identificacao_aih": "1",
            "indicador_obito": True,
            "dias_permanencia_real": 4,  # ?bito ap?s 4 dias n?o ? perman?ncia zero
            "valor_total_brl": 2500.0,
        },
        {
            "numero_aih": "5555555555555",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "CE",
            "codigo_procedimento_realizado": "0303010037",
            "tipo_identificacao_aih": "1",
            "indicador_obito": False,
            "dias_permanencia_real": 0,  # Alta no mesmo dia sem ?bito n?o ? anomalia de ?bito
            "valor_total_brl": 300.0,
        },
    ])
    
    sql = get_central_anomalias_sql(fct_internacao_source="df_raw", min_proc_count=1)
    df_res = conn.execute(sql).df()
    conn.close()

    obitos_imediatos = df_res[df_res["tipo_anomalia"] == "OBITO_PERMANENCIA_ZERO"]
    assert len(obitos_imediatos) == 1, "Apenas o ?bito com dias_permanencia_real = 0 deve disparar OBITO_PERMANENCIA_ZERO."
    assert obitos_imediatos.iloc[0]["numero_aih"] == "3333333333333"
    assert obitos_imediatos.iloc[0]["severidade"] == "CRITICA"


def test_build_aud_alertas_anomalias_materializacao():
    """Valida a materializa??o f?sica da tabela aud_alertas_anomalias com chaves ?nicas."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {
            "numero_aih": "9999999999991",
            "codigo_estabelecimento_cnes": "2077485",
            "uf": "SP",
            "codigo_procedimento_realizado": "0408050160",
            "tipo_identificacao_aih": "1",
            "indicador_obito": True,
            "dias_permanencia_real": 0,
            "valor_total_brl": 0.0,
        }
    ])
    
    conn.register("fct_internacao", df_raw)
    total_alertas = build_aud_alertas_anomalias(conn, fct_internacao_source="fct_internacao", target_table="aud_alertas_anomalias")
    
    df_aud = conn.execute("SELECT * FROM aud_alertas_anomalias").df()
    conn.close()

    assert total_alertas > 0
    assert df_aud["id_alerta"].nunique() == len(df_aud), "Todos os id_alerta devem ser ?nicos (PK)."
    assert "status_operacional" in df_aud.columns
    assert (df_aud["status_operacional"] == "NOVA").all()
