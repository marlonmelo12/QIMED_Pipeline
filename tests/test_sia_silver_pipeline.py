"""
Testes Automatizados da Fato Ambulatorial SIA (Tasks 2, 3 e 12).
"""
import os
import duckdb
import pytest
import pandas as pd


def test_surrogate_key_unicidade_sia():
    """Valida que id_atendimento_ambulatorial ? ?nico para cada linha gerada [Task 2]."""
    conn = duckdb.connect()
    
    # Amostra simulando faturamentos BPA com dados repetidos
    df_raw = pd.DataFrame([
        {"ano": "2026", "mes": "05", "PA_CODUNI": "2077485", "PA_PROC_ID": "0301010072", "PA_CIDPRI": "0000", "PA_SEXO": "F", "PA_IDADE": "35", "PA_MUNPCN": "230440"},
        {"ano": "2026", "mes": "05", "PA_CODUNI": "2077485", "PA_PROC_ID": "0301010072", "PA_CIDPRI": "0000", "PA_SEXO": "F", "PA_IDADE": "35", "PA_MUNPCN": "230440"},
        {"ano": "2026", "mes": "05", "PA_CODUNI": "2077485", "PA_PROC_ID": "0301010072", "PA_CIDPRI": "0000", "PA_SEXO": "F", "PA_IDADE": "35", "PA_MUNPCN": "230440"},
    ])
    
    df_res = conn.execute("""
        SELECT 
            md5(concat_ws('-', 
                'CE', 
                COALESCE(ano, ''), 
                COALESCE(mes, ''), 
                COALESCE(PA_CODUNI, ''), 
                COALESCE(PA_PROC_ID, ''), 
                COALESCE(PA_CIDPRI, ''), 
                COALESCE(PA_SEXO, ''), 
                COALESCE(PA_IDADE, ''), 
                COALESCE(PA_MUNPCN, ''), 
                ROW_NUMBER() OVER ()
            )) AS id_atendimento_ambulatorial
        FROM df_raw
    """).df()
    conn.close()

    assert len(df_res) == 3
    assert df_res["id_atendimento_ambulatorial"].nunique() == 3, "Cada linha de faturamento ambulatorial deve possuir PK ?nica."


def test_sanitizacao_idade_sentinela():
    """Valida que PA_IDADE = 999 ou negativos viram NULL, enquanto idades v?lidas s?o mantidas [Task 12]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {"PA_IDADE": "999"},
        {"PA_IDADE": "-1"},
        {"PA_IDADE": "0"},
        {"PA_IDADE": "45"},
        {"PA_IDADE": "82"},
        {"PA_IDADE": None},
        {"PA_IDADE": ""},
    ])
    
    df_res = conn.execute("""
        SELECT 
            CASE 
                WHEN TRY_CAST(PA_IDADE AS INTEGER) = 999 OR TRY_CAST(PA_IDADE AS INTEGER) < 0 THEN NULL 
                ELSE TRY_CAST(PA_IDADE AS INTEGER) 
            END AS idade_paciente_anos
        FROM df_raw
    """).df()
    conn.close()

    idades = df_res["idade_paciente_anos"].tolist()
    assert pd.isna(idades[0]), "Idade 999 deve ser convertida para NULL."
    assert pd.isna(idades[1]), "Idade negativa deve ser convertida para NULL."
    assert idades[2] == 0, "Rec?m-nascido (idade 0) deve ser mantido como 0."
    assert idades[3] == 45, "Idade v?lida (45) deve ser preservada."
    assert idades[4] == 82, "Idade v?lida (82) deve ser preservada."
    assert pd.isna(idades[5]), "None deve permanecer NULL."
    assert pd.isna(idades[6]), "String vazia deve ser convertida para NULL."


def test_sanitizacao_cid_sentinela():
    """Valida que c?digos '0000', '0', '', 'NONE', 'nan' viram NULL [Task 12]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {"PA_CIDPRI": "0000"},
        {"PA_CIDPRI": "0"},
        {"PA_CIDPRI": ""},
        {"PA_CIDPRI": "NONE"},
        {"PA_CIDPRI": "nan"},
        {"PA_CIDPRI": "I10"},
        {"PA_CIDPRI": "E11.9"},
    ])
    
    df_res = conn.execute("""
        SELECT 
            CASE 
                WHEN TRIM(CAST(PA_CIDPRI AS VARCHAR)) IN ('0000', '0', '', 'NONE', 'NULL', 'nan') THEN NULL 
                ELSE TRIM(CAST(PA_CIDPRI AS VARCHAR)) 
            END AS codigo_cid10_principal
        FROM df_raw
    """).df()
    conn.close()

    cids = df_res["codigo_cid10_principal"].tolist()
    assert pd.isna(cids[0]), "CID '0000' deve virar NULL."
    assert pd.isna(cids[1]), "CID '0' deve virar NULL."
    assert pd.isna(cids[2]), "CID vazio deve virar NULL."
    assert pd.isna(cids[3]), "CID 'NONE' deve virar NULL."
    assert pd.isna(cids[4]), "CID 'nan' deve virar NULL."
    assert cids[5] == "I10", "CID 'I10' deve ser preservado."
    assert cids[6] == "E11.9", "CID 'E11.9' deve ser preservado."


def test_procedimentos_valor_zero_pab_preservados():
    """Valida que procedimentos PAB de valor 0.0 s?o preservados com 0.0 e n?o viram NULL [Task 3/12]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {"PA_VALAPR": "0.00"},
        {"PA_VALAPR": "0"},
        {"PA_VALAPR": "150.50"},
        {"PA_VALAPR": None},
    ])
    
    df_res = conn.execute("""
        SELECT 
            COALESCE(TRY_CAST(PA_VALAPR AS DOUBLE), 0.0) AS valor_aprovado_brl
        FROM df_raw
    """).df()
    conn.close()

    valores = df_res["valor_aprovado_brl"].tolist()
    assert valores[0] == 0.0, "Procedimento PAB de R$ 0,00 deve ser 0.0."
    assert valores[1] == 0.0, "Procedimento PAB de R$ 0 deve ser 0.0."
    assert valores[2] == 150.50, "Procedimento faturado R$ 150,50 deve ser 150.50."
    assert valores[3] == 0.0, "Valor ausente deve receber fallback seguro 0.0."
