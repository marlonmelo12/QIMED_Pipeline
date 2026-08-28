"""
Testes Automatizados das Views Semânticas (Consolidação e ML) e Guardrails Clínicos na API.
"""
import os
import duckdb
import pytest

from src.api.guardrails import ClinicalInferenceGuardrails

DB_PATH = "warehouse/qimed_silver_completa.duckdb"


def test_unicidade_vw_internacoes_consolidadas():
    """Validar que COUNT(DISTINCT numero_aih) == COUNT(*) na view consolidada (exatamente 1.250.192 linhas 1:1)."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} não encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("""
        SELECT 
            COUNT(*) AS total_linhas,
            COUNT(DISTINCT numero_aih) AS total_aihs
        FROM vw_internacoes_consolidadas
    """).fetchone()
    conn.close()

    total_linhas, total_aihs = res
    assert total_linhas == 1250192, f"Esperavam-se 1.250.192 linhas consolidadas, mas foram encontradas {total_linhas}."
    assert total_linhas == total_aihs, f"A view consolidada contém duplicatas de AIH: {total_linhas} linhas vs {total_aihs} AIHs únicas."


def test_anti_leakage_vw_ml_treinamento_admissao():
    """Validar que a view de ML contém apenas AIHs Tipo 1 e não projeta motivo_saida, data_alta ou codigo_cid10_secundario nas features."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} não encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # 1. Checar volumetria de AIH tipo 1
    total_linhas = conn.execute("SELECT COUNT(*) FROM vw_ml_treinamento_admissao").fetchone()[0]
    
    # 2. Checar ausência de colunas pós-alta (data leakage)
    df_desc = conn.execute("DESCRIBE vw_ml_treinamento_admissao").df()
    conn.close()

    colunas = df_desc["column_name"].str.lower().tolist()
    colunas_proibidas_leakage = ["motivo_saida", "data_alta", "codigo_cid10_secundario", "dias_permanencia_faturados_mes"]
    
    for col in colunas_proibidas_leakage:
        assert col not in colunas, f"Vazamento de dados (data leakage) detectado! Coluna proibida '{col}' está presente na view de ML."

    assert total_linhas == 1239452, f"Esperavam-se 1.239.452 linhas na view de ML (AIH 1), mas foram encontradas {total_linhas}."


def test_ml_features_and_targets_isolation():
    """Valida o desacoplamento estrito entre features de admissão e targets de desfecho."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} não encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # 1. Checa view de features
    df_feat = conn.execute("DESCRIBE vw_ml_features_admissao").df()
    cols_feat = df_feat["column_name"].str.lower().tolist()
    assert "target_obito_hospitalar" not in cols_feat
    assert "target_longa_permanencia_11d" not in cols_feat
    assert "target_alto_custo_p90" not in cols_feat
    assert "idade_admissao_anos" in cols_feat
    assert "codigo_cid10_principal" in cols_feat

    # 2. Checa view de targets
    df_tgt = conn.execute("DESCRIBE vw_ml_targets_internacao").df()
    cols_tgt = df_tgt["column_name"].str.lower().tolist()
    assert "target_obito_hospitalar" in cols_tgt
    assert "target_longa_permanencia_11d" in cols_tgt
    assert "target_alto_custo_p90" in cols_tgt
    assert "idade_admissao_anos" not in cols_tgt
    assert "sexo_biologico" not in cols_tgt

    conn.close()


def test_guardrail_clinico_homem_obstetricia():
    """Testar que o middleware anula predições obstétricas para sexo = 'M'."""
    payload_homem_parto = {
        "sexo_biologico": "M",
        "codigo_cid10_principal": "O80.0",
        "idade_admissao_anos": 32
    }
    predicao_bruta = {
        "probabilidade_obito": 0.02,
        "probabilidade_longa_permanencia": 0.15,
        "risco_obstetrico_ajustado": 0.88,
        "confiabilidade_predicao": "ALTA"
    }
    
    predicao_sanitizada = ClinicalInferenceGuardrails.sanitizar_predicao(payload_homem_parto, predicao_bruta)
    
    assert predicao_sanitizada.get("alerta_clinico") == "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
    assert predicao_sanitizada.get("risco_obstetrico_ajustado") == 0.0
    assert predicao_sanitizada.get("confiabilidade_predicao") == "BAIXA_ERRO_CADASTRO"


def test_guardrail_clinico_homem_endometriose_e_prenatal():
    """Testar que o middleware anula predições para CIDs ginecológicos adicionais (N80, Z34) em homens."""
    for cid in ["N80.0", "Z34.8", "C54.1"]:
        payload = {"sexo_biologico": "M", "codigo_cid10_principal": cid}
        pred_bruta = {"risco_obstetrico_ajustado": 0.75, "confiabilidade_predicao": "ALTA"}
        sanitizada = ClinicalInferenceGuardrails.sanitizar_predicao(payload, pred_bruta)
        assert sanitizada.get("alerta_clinico") == "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
        assert sanitizada.get("risco_obstetrico_ajustado") == 0.0


def test_guardrail_clinico_mulher_prostata():
    """Testar que o middleware anula predições prostáticas para sexo = 'F'."""
    payload_mulher_prostata = {
        "sexo_biologico": "F",
        "codigo_cid10_principal": "C61",
        "idade_admissao_anos": 68
    }
    predicao_bruta = {
        "probabilidade_obito": 0.12,
        "probabilidade_longa_permanencia": 0.45,
        "risco_urologico_masculino_ajustado": 0.95,
        "confiabilidade_predicao": "ALTA"
    }
    
    predicao_sanitizada = ClinicalInferenceGuardrails.sanitizar_predicao(payload_mulher_prostata, predicao_bruta)
    
    assert predicao_sanitizada.get("alerta_clinico") == "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
    assert predicao_sanitizada.get("risco_urologico_masculino_ajustado") == 0.0
    assert predicao_sanitizada.get("confiabilidade_predicao") == "BAIXA_ERRO_CADASTRO"


def test_guardrail_clinico_mulher_penis_e_testiculo():
    """Testar que o middleware anula predições para neoplasias masculinas (C60, C62) em mulheres."""
    for cid in ["C60.9", "C62.1", "N45.0"]:
        payload = {"sexo_biologico": "F", "codigo_cid10_principal": cid}
        pred_bruta = {"risco_urologico_masculino_ajustado": 0.90, "confiabilidade_predicao": "ALTA"}
        sanitizada = ClinicalInferenceGuardrails.sanitizar_predicao(payload, pred_bruta)
        assert sanitizada.get("alerta_clinico") == "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
        assert sanitizada.get("risco_urologico_masculino_ajustado") == 0.0


def test_guardrail_clinico_sexo_ignorado():
    """Testar tratamento especial para sexo = 'I' (Ignorado)."""
    payload = {"sexo_biologico": "I", "codigo_cid10_principal": "I10"}
    pred_bruta = {"probabilidade_obito": 0.05, "confiabilidade_predicao": "ALTA"}
    sanitizada = ClinicalInferenceGuardrails.sanitizar_predicao(payload, pred_bruta)
    assert sanitizada.get("alerta_clinico") == "SEXO_INDETERMINADO_INFERENCIA_RESTRITA"
    assert sanitizada.get("confiabilidade_predicao") == "MEDIA_SEXO_IGNORADO"
