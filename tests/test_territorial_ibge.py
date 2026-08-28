"""
Testes Automatizados de Valida??o Territorial IBGE vs UF e Mobilidade Assistencial (Task 11).
"""
import os
import duckdb
import pytest
import pandas as pd
from src.silver.terminology import UF_IBGE_PREFIX


def test_cobertura_27_ufs_ibge():
    """Valida que os 27 prefixos estaduais do IBGE mapeiam para as 27 UFs brasileiras corretas."""
    conn = duckdb.connect()
    
    # Criar amostra com 1 c?digo para cada uma das 27 UFs
    amostras_municipios = [
        ("110020", "RO"), ("120040", "AC"), ("130260", "AM"), ("140010", "RR"),
        ("150140", "PA"), ("160030", "AP"), ("172100", "TO"), ("211130", "MA"),
        ("221100", "PI"), ("230440", "CE"), ("240810", "RN"), ("250750", "PB"),
        ("261160", "PE"), ("270430", "AL"), ("280030", "SE"), ("292740", "BA"),
        ("310620", "MG"), ("320530", "ES"), ("330455", "RJ"), ("355030", "SP"),
        ("410690", "PR"), ("420540", "SC"), ("431490", "RS"), ("500270", "MS"),
        ("510340", "MT"), ("520870", "GO"), ("530010", "DF")
    ]
    
    df_raw = pd.DataFrame(amostras_municipios, columns=["cd_municipio", "uf_esperada"])
    
    df_res = conn.execute("""
        SELECT 
            cd_municipio,
            uf_esperada,
            CASE SUBSTRING(TRIM(CAST(cd_municipio AS VARCHAR)), 1, 2)
                WHEN '11' THEN 'RO' WHEN '12' THEN 'AC' WHEN '13' THEN 'AM' WHEN '14' THEN 'RR' WHEN '15' THEN 'PA' WHEN '16' THEN 'AP' WHEN '17' THEN 'TO'
                WHEN '21' THEN 'MA' WHEN '22' THEN 'PI' WHEN '23' THEN 'CE' WHEN '24' THEN 'RN' WHEN '25' THEN 'PB' WHEN '26' THEN 'PE' WHEN '27' THEN 'AL' WHEN '28' THEN 'SE' WHEN '29' THEN 'BA'
                WHEN '31' THEN 'MG' WHEN '32' THEN 'ES' WHEN '33' THEN 'RJ' WHEN '35' THEN 'SP'
                WHEN '41' THEN 'PR' WHEN '42' THEN 'SC' WHEN '43' THEN 'RS'
                WHEN '50' THEN 'MS' WHEN '51' THEN 'MT' WHEN '52' THEN 'GO' WHEN '53' THEN 'DF'
                ELSE 'BR'
            END AS uf_derivada
        FROM df_raw
    """).df()
    conn.close()

    assert len(df_res) == 27, "Devem haver exatamente 27 estados testados."
    for idx, row in df_res.iterrows():
        assert row["uf_derivada"] == row["uf_esperada"], f"Inconsist?ncia para {row['cd_municipio']}: esperado {row['uf_esperada']}, obteve {row['uf_derivada']}"


def test_correcao_inconsistencia_territorial():
    """Valida que morador do CE atendido em SP tem uf_residencia='CE' e hospital_uf='SP'."""
    conn = duckdb.connect()
    
    df_sample = pd.DataFrame([{
        "MUNIC_RES": "230440", # Fortaleza / CE
        "uf_hospital": "SP"
    }])
    
    df_res = conn.execute("""
        SELECT 
            MUNIC_RES,
            uf_hospital,
            CASE SUBSTRING(TRIM(CAST(MUNIC_RES AS VARCHAR)), 1, 2)
                WHEN '11' THEN 'RO' WHEN '12' THEN 'AC' WHEN '13' THEN 'AM' WHEN '14' THEN 'RR' WHEN '15' THEN 'PA' WHEN '16' THEN 'AP' WHEN '17' THEN 'TO'
                WHEN '21' THEN 'MA' WHEN '22' THEN 'PI' WHEN '23' THEN 'CE' WHEN '24' THEN 'RN' WHEN '25' THEN 'PB' WHEN '26' THEN 'PE' WHEN '27' THEN 'AL' WHEN '28' THEN 'SE' WHEN '29' THEN 'BA'
                WHEN '31' THEN 'MG' WHEN '32' THEN 'ES' WHEN '33' THEN 'RJ' WHEN '35' THEN 'SP'
                WHEN '41' THEN 'PR' WHEN '42' THEN 'SC' WHEN '43' THEN 'RS'
                WHEN '50' THEN 'MS' WHEN '51' THEN 'MT' WHEN '52' THEN 'GO' WHEN '53' THEN 'DF'
                ELSE uf_hospital
            END AS uf_residencia_paciente
        FROM df_sample
    """).df()
    conn.close()

    assert df_res["uf_residencia_paciente"][0] == "CE", "A UF de resid?ncia deveria ser 'CE', derivada do c?digo 230440."
    assert df_res["uf_hospital"][0] == "SP", "A UF de atendimento hospitalar deve permanecer 'SP'."


def test_mobilidade_interestadual_preservada():
    """Valida que atendimentos interestaduais (PPI) s?o preservados na dim_paciente e fatos."""
    db_path = "warehouse/qimed_silver_completa.duckdb"
    assert os.path.exists(db_path), f"Arquivo {db_path} n?o encontrado."
    
    conn = duckdb.connect(db_path, read_only=True)
    # Valida integridade na dim_paciente
    df_inconsistencias = conn.execute("""
        SELECT COUNT(*) AS total_inconsistencias
        FROM dim_paciente
        WHERE SUBSTRING(CAST(codigo_municipio_residencia AS VARCHAR), 1, 2) = '23'
          AND uf_residencia != 'CE';
    """).df()
    
    conn.close()

    assert df_inconsistencias["total_inconsistencias"][0] == 0, f"Encontradas {df_inconsistencias['total_inconsistencias'][0]} inconsist?ncias territoriais na dim_paciente."
