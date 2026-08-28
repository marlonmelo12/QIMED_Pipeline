"""
Testes Automatizados da Fato Interna??o SIH e Resolu??o Cl?nica MPI (Tasks 5 e 7).
"""
import os
import duckdb
import pytest
import pandas as pd
from src.mpi.patient_identity import PatientIdentityResolver


def test_sanitizacao_cid_secundario_sih():
    """Valida que DIAG_SECUN = '0000', '0', '' viram NULL no SIH [Task 7]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([
        {"DIAG_SECUN": "0000"},
        {"DIAG_SECUN": "0"},
        {"DIAG_SECUN": ""},
        {"DIAG_SECUN": "NONE"},
        {"DIAG_SECUN": "nan"},
        {"DIAG_SECUN": "I10"},
        {"DIAG_SECUN": "E11.9"},
    ])
    
    df_res = conn.execute("""
        SELECT 
            CASE 
                WHEN TRIM(CAST(DIAG_SECUN AS VARCHAR)) IN ('0000', '0', '', 'NONE', 'NULL', 'nan') THEN NULL 
                ELSE TRIM(CAST(DIAG_SECUN AS VARCHAR)) 
            END AS codigo_cid10_secundario
        FROM df_raw
    """).df()
    conn.close()

    cids = df_res["codigo_cid10_secundario"].tolist()
    assert pd.isna(cids[0]), "CID secund?rio '0000' deve virar NULL."
    assert pd.isna(cids[1]), "CID secund?rio '0' deve virar NULL."
    assert pd.isna(cids[2]), "CID secund?rio vazio deve virar NULL."
    assert pd.isna(cids[3]), "CID secund?rio 'NONE' deve virar NULL."
    assert pd.isna(cids[4]), "CID secund?rio 'nan' deve virar NULL."
    assert cids[5] == "I10", "CID secund?rio 'I10' deve ser preservado."
    assert cids[6] == "E11.9", "CID secund?rio 'E11.9' deve ser preservado."


def test_preservacao_campos_continuidade_aih5():
    """Valida a preserva??o de campos de continuidade (SEQ_AIH5, NUM_DOC, MOTSAID, AIH_PROX, VAL_SH, VAL_SP, PROC_SOLIC) [Task 5]."""
    conn = duckdb.connect()
    
    df_raw = pd.DataFrame([{
        "N_AIH": "1234567890123",
        "NUM_DOC": "LAUDO9988",
        "N_DOC": None,
        "SEQ_AIH5": "2",
        "IDENT": "5",
        "MOTSAID": "51",
        "AIH_PROX": "1234567890124",
        "VAL_SH": "1250.75",
        "VAL_SP": "450.25",
        "PROC_REA": "0303010037",
        "PROC_SOLIC": "0303010010",
        "CNES": "2077485",
        "MUNIC_RES": "230440",
        "MUNIC_MOV": "230440",
        "NASC": "19800515",
        "SEXO": "1",
        "DT_INTER": "20260501",
        "DT_SAIDA": "20260531",
        "DIAG_PRINC": "I210",
        "DIAG_SECUN": "0000",
        "DIAS_PERM": "30",
        "MORTE": "0",
        "VAL_TOT": "1701.00",
        "VAL_UTI": "0.0",
        "ano": "2026",
        "mes": "05",
        "uf": "CE",
        "id_execucao": "exec_test"
    }])
    
    df_res = conn.execute("""
        SELECT
            N_AIH AS numero_aih,
            COALESCE(TRY_CAST(NUM_DOC AS VARCHAR), TRY_CAST(N_DOC AS VARCHAR), NULL) AS numero_documento_autorizacao,
            TRY_CAST(SEQ_AIH5 AS INTEGER) AS sequencial_aih5,
            IDENT AS tipo_identificacao_aih,
            TRY_CAST(MOTSAID AS VARCHAR) AS motivo_saida,
            TRY_CAST(AIH_PROX AS VARCHAR) AS numero_aih_proxima,
            TRY_CAST(VAL_SH AS DOUBLE) AS valor_servicos_hospitalares_brl,
            TRY_CAST(VAL_SP AS DOUBLE) AS valor_servicos_profissionais_brl,
            PROC_REA AS codigo_procedimento_realizado,
            COALESCE(TRY_CAST(PROC_SOLIC AS VARCHAR), PROC_REA) AS codigo_procedimento_solicitado
        FROM df_raw
    """).df()
    conn.close()

    row = df_res.iloc[0]
    assert row["numero_documento_autorizacao"] == "LAUDO9988"
    assert row["sequencial_aih5"] == 2
    assert row["tipo_identificacao_aih"] == "5"
    assert row["motivo_saida"] == "51"
    assert row["numero_aih_proxima"] == "1234567890124"
    assert row["valor_servicos_hospitalares_brl"] == 1250.75
    assert row["valor_servicos_profissionais_brl"] == 450.25
    assert row["codigo_procedimento_solicitado"] == "0303010010"


def test_desambiguacao_neonatal_mpi():
    """Valida que rec?m-nascidos distintos nascidos no mesmo dia e munic?pio N?O sofrem colapso de pseud?nimo [Task 5]."""
    resolver = PatientIdentityResolver()
    conn = duckdb.connect()

    # Dois rec?m-nascidos distintos (AIHs diferentes) que nasceram no dia da interna??o no mesmo hospital/munic?pio
    df_neo = pd.DataFrame([
        {
            "numero_aih": "1111111111111",
            "numero_documento_autorizacao": None,
            "codigo_estabelecimento_cnes": "2077485",
            "data_internacao": "20260501",
            "data_nascimento_paciente": "20260501",
            "sexo_biologico": "M",
            "codigo_municipio_residencia_paciente": "230440",
        },
        {
            "numero_aih": "2222222222222",
            "numero_documento_autorizacao": None,
            "codigo_estabelecimento_cnes": "2077485",
            "data_internacao": "20260501",
            "data_nascimento_paciente": "20260501",
            "sexo_biologico": "M",
            "codigo_municipio_residencia_paciente": "230440",
        },
        # Um adulto comum nascido em data diferente da interna??o
        {
            "numero_aih": "3333333333333",
            "numero_documento_autorizacao": None,
            "codigo_estabelecimento_cnes": "2077485",
            "data_internacao": "20260510",
            "data_nascimento_paciente": "19850420",
            "sexo_biologico": "F",
            "codigo_municipio_residencia_paciente": "230440",
        },
    ])

    sql_resolved = resolver.resolve_identities_sql("SELECT * FROM df_neo")
    df_res = conn.execute(sql_resolved).df()
    conn.close()

    pseudos = df_res["pseudonimo_paciente"].tolist()
    candidatos = df_res["identificador_paciente_candidato"].tolist()

    # Beb? 1 vs Beb? 2: NUNCA devem ter o mesmo pseud?nimo nem o mesmo identificador candidato
    assert pseudos[0] != pseudos[1], f"Colapso detectado entre rec?m-nascidos: {pseudos[0]} == {pseudos[1]}"
    assert candidatos[0] != candidatos[1], f"Colapso de cluster candidato entre rec?m-nascidos: {candidatos[0]} == {candidatos[1]}"
    
    # Adulto deve ser ?nico
    assert pseudos[2] != pseudos[0]
    assert pseudos[2] != pseudos[1]
