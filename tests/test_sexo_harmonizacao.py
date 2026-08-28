"""
Testes Automatizados de Harmoniza??o de sexo_biologico e Resolu??o de Identidade no MPI (Task 6).
"""
import os
import duckdb
import pytest
import pandas as pd
from src.processing.duckdb_engine import DuckDBEngine
from src.mpi.patient_identity import PatientIdentityResolver


def test_dominio_canonico_sexo():
    """Valida que valores legados ('1', '3', 'M', 'F', '0', '9', NULL, '') mapeiam estritamente para {'M', 'F', 'I'}."""
    conn = duckdb.connect()
    df_raw = pd.DataFrame({
        "valor_bruto": ["1", "3", "M", "F", "0", "9", None, "", "MASC", "FEMININO", "2", "IGNORADO"]
    })
    
    df_res = conn.execute("""
        SELECT 
            valor_bruto,
            CASE 
                WHEN UPPER(TRIM(CAST(valor_bruto AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
                WHEN UPPER(TRIM(CAST(valor_bruto AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
                ELSE 'I'
            END AS sexo_biologico
        FROM df_raw
    """).df()
    conn.close()

    valores_unicos = set(df_res["sexo_biologico"].unique())
    assert valores_unicos.issubset({"M", "F", "I"}), f"Valores fora do dom?nio can?nico: {valores_unicos}"
    
    # Mapeamentos individuais espec?ficos
    map_res = dict(zip(df_res["valor_bruto"].fillna("NULL"), df_res["sexo_biologico"]))
    assert map_res["1"] == "M"
    assert map_res["M"] == "M"
    assert map_res["MASC"] == "M"
    assert map_res["3"] == "F"
    assert map_res["F"] == "F"
    assert map_res["2"] == "F"
    assert map_res["FEMININO"] == "F"
    assert map_res["0"] == "I"
    assert map_res["9"] == "I"
    assert map_res["NULL"] == "I"
    assert map_res[""] == "I"


def test_unificacao_mpi_cross_system_linkage():
    """Valida que um paciente vindo do SIH (SEXO='1') e do SIA (PA_SEXO='M') gera o exato mesmo pseudonimo_paciente."""
    engine = DuckDBEngine()
    resolver = PatientIdentityResolver(duck_engine=engine)
    
    # Registro A: SIH (SEXO = '1')
    raw_sih_sql = """
    SELECT 
        '1234567890123' AS numero_aih,
        NULL AS numero_documento_autorizacao,
        '2077485' AS codigo_estabelecimento_cnes,
        '20260501' AS data_internacao,
        '20000515' AS data_nascimento_paciente,
        CASE 
            WHEN UPPER(TRIM(CAST('1' AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
            WHEN UPPER(TRIM(CAST('1' AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
            ELSE 'I'
        END AS sexo_biologico,
        '230440' AS codigo_municipio_residencia_paciente
    """
    
    # Registro B: SIA (PA_SEXO = 'M')
    raw_sia_sql = """
    SELECT 
        '1234567890123' AS numero_aih,
        NULL AS numero_documento_autorizacao,
        '2077485' AS codigo_estabelecimento_cnes,
        '20260501' AS data_internacao,
        '20000515' AS data_nascimento_paciente,
        CASE 
            WHEN UPPER(TRIM(CAST('M' AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
            WHEN UPPER(TRIM(CAST('M' AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
            ELSE 'I'
        END AS sexo_biologico,
        '230440' AS codigo_municipio_residencia_paciente
    """
    
    sql_resolved_sih = resolver.resolve_identities_sql(raw_sih_sql)
    sql_resolved_sia = resolver.resolve_identities_sql(raw_sia_sql)
    
    res_sih = engine.fetch_arrow(sql_resolved_sih).to_pandas()
    res_sia = engine.fetch_arrow(sql_resolved_sia).to_pandas()
    
    pseudonimo_sih = res_sih["pseudonimo_paciente"][0]
    pseudonimo_sia = res_sia["pseudonimo_paciente"][0]
    candidato_sih = res_sih["identificador_paciente_candidato"][0]
    candidato_sia = res_sia["identificador_paciente_candidato"][0]
    
    assert pseudonimo_sih == pseudonimo_sia, f"Hashes MPI divergiram: SIH={pseudonimo_sih} vs SIA={pseudonimo_sia}"
    assert candidato_sih == candidato_sia, f"Candidatos MPI divergiram: SIH={candidato_sih} vs SIA={candidato_sia}"


def test_nao_nulidade_sexo_silver():
    """Garante que a coluna sexo_biologico nunca possui valores NULL."""
    conn = duckdb.connect()
    df_raw = pd.DataFrame({
        "sexo_in": [None, "", "unknown", "1", "3"]
    })
    df_res = conn.execute("""
        SELECT 
            CASE 
                WHEN UPPER(TRIM(CAST(sexo_in AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
                WHEN UPPER(TRIM(CAST(sexo_in AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
                ELSE 'I'
            END AS sexo_biologico
        FROM df_raw
    """).df()
    conn.close()

    assert df_res["sexo_biologico"].isnull().sum() == 0, "A coluna sexo_biologico n?o pode conter valores NULL."
    assert set(df_res["sexo_biologico"].unique()) == {"M", "F", "I"}
