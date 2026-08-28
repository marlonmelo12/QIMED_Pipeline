"""
Testes Automatizados de Encoding e Ausencia de Mojibake na ANS (Task de Higienizacao de Encoding).
"""
import os
import duckdb
import pytest
from src.collectors.ans_collector import AnsCollector


def test_ans_collector_decode_safe():
    """Valida que _decode_content_safe trata corretamente bytes UTF-8 e corrige latin-1 encapsulado."""
    collector = AnsCollector(modalidade="operadoras")
    
    # 1. Teste com bytes UTF-8 genuinos
    texto_original = "ALLIANZ SA\u00daDE S/A - ASSOCIA\u00c7\u00c3O BENEFICENTE"
    utf8_bytes = texto_original.encode("utf-8")
    res_utf8 = collector._decode_content_safe(utf8_bytes)
    assert res_utf8 == texto_original

    # 2. Teste com mojibake latin-1
    mojibake_bytes = texto_original.encode("utf-8").decode("latin-1").encode("latin-1")
    res_fixed = collector._decode_content_safe(mojibake_bytes)
    assert "SA\u00daDE" in res_fixed
    assert "\u00c3\u009a" not in res_fixed


def test_ausencia_mojibake_dim_operadoras_saude():
    """Valida que dim_operadoras_saude nao possui nenhuma sequencia de mojibake nem modalidade nula."""
    db_path = "warehouse/qimed_silver_completa.duckdb"
    assert os.path.exists(db_path), f"Arquivo {db_path} nao encontrado."

    conn = duckdb.connect(db_path, read_only=True)
    df = conn.execute("""
        SELECT 
            COUNT(*) AS total_linhas,
            SUM(CASE WHEN razao_social LIKE '%Ã\x8d%' OR razao_social LIKE '%Ã\x93%' OR razao_social LIKE '%Ã\x9a%' OR razao_social LIKE '%Ã\x87%' OR razao_social LIKE '%Ã\x83%' OR razao_social LIKE '%Ãº%' OR razao_social LIKE '%Ã§%' OR razao_social LIKE '%Ã£%' OR razao_social LIKE '%Ã©%' OR razao_social LIKE '%Ã¡%' OR razao_social LIKE '%â€%' THEN 1 ELSE 0 END) AS mojibake_razao,
            SUM(CASE WHEN modalidade_operadora LIKE '%Ã\x8d%' OR modalidade_operadora LIKE '%Ã\x93%' OR modalidade_operadora LIKE '%Ã\x9a%' OR modalidade_operadora LIKE '%Ã\x87%' OR modalidade_operadora LIKE '%Ã\x83%' OR modalidade_operadora LIKE '%Ãº%' OR modalidade_operadora LIKE '%Ã§%' OR modalidade_operadora LIKE '%Ã£%' OR modalidade_operadora LIKE '%Ã©%' OR modalidade_operadora LIKE '%Ã¡%' OR modalidade_operadora LIKE '%â€%' THEN 1 ELSE 0 END) AS mojibake_modalidade,
            SUM(CASE WHEN modalidade_operadora IS NULL OR TRIM(modalidade_operadora) = '' THEN 1 ELSE 0 END) AS modalidade_nula
        FROM dim_operadoras_saude;
    """).df()
    conn.close()

    assert df["total_linhas"][0] == 1115, f"Esperado 1115 operadoras, obteve {df['total_linhas'][0]}"
    assert df["mojibake_razao"][0] == 0, f"Encontradas {df['mojibake_razao'][0]} operadoras com mojibake na razao social."
    assert df["mojibake_modalidade"][0] == 0, f"Encontradas {df['mojibake_modalidade'][0]} operadoras com mojibake na modalidade."
    assert df["modalidade_nula"][0] == 0, f"Encontradas {df['modalidade_nula'][0]} operadoras com modalidade nula."


def test_ausencia_mojibake_fct_ressarcimento_sus():
    """Valida que fct_ressarcimento_sus nao possui mojibake na razao social ou modalidade."""
    db_path = "warehouse/qimed_silver_completa.duckdb"
    assert os.path.exists(db_path), f"Arquivo {db_path} nao encontrado."

    conn = duckdb.connect(db_path, read_only=True)
    df = conn.execute("""
        SELECT 
            COUNT(*) AS total_linhas,
            SUM(CASE WHEN razao_social_operadora LIKE '%Ã\x8d%' OR razao_social_operadora LIKE '%Ã\x93%' OR razao_social_operadora LIKE '%Ã\x9a%' OR razao_social_operadora LIKE '%Ã\x87%' OR razao_social_operadora LIKE '%Ã\x83%' OR razao_social_operadora LIKE '%Ãº%' OR razao_social_operadora LIKE '%Ã§%' OR razao_social_operadora LIKE '%Ã£%' OR razao_social_operadora LIKE '%Ã©%' OR razao_social_operadora LIKE '%Ã¡%' OR razao_social_operadora LIKE '%â€%' THEN 1 ELSE 0 END) AS mojibake_razao,
            SUM(CASE WHEN modalidade_operadora LIKE '%Ã\x8d%' OR modalidade_operadora LIKE '%Ã\x93%' OR modalidade_operadora LIKE '%Ã\x9a%' OR modalidade_operadora LIKE '%Ã\x87%' OR modalidade_operadora LIKE '%Ã\x83%' OR modalidade_operadora LIKE '%Ãº%' OR modalidade_operadora LIKE '%Ã§%' OR modalidade_operadora LIKE '%Ã£%' OR modalidade_operadora LIKE '%Ã©%' OR modalidade_operadora LIKE '%Ã¡%' OR modalidade_operadora LIKE '%â€%' THEN 1 ELSE 0 END) AS mojibake_modalidade,
            SUM(CASE WHEN modalidade_operadora IS NULL OR TRIM(modalidade_operadora) = '' THEN 1 ELSE 0 END) AS modalidade_nula
        FROM fct_ressarcimento_sus;
    """).df()
    conn.close()

    assert df["total_linhas"][0] > 0
    assert df["mojibake_razao"][0] == 0, f"Encontrados {df['mojibake_razao'][0]} registros de ressarcimento com mojibake na razao social."
    assert df["mojibake_modalidade"][0] == 0, f"Encontrados {df['mojibake_modalidade'][0]} registros de ressarcimento com mojibake na modalidade."
    assert df["modalidade_nula"][0] == 0, f"Encontrados {df['modalidade_nula'][0]} registros de ressarcimento com modalidade nula."


def test_operadoras_conhecidas_acentuacao_correta():
    """Valida operadoras conhecidas no banco com acentuacao correta em portugues."""
    db_path = "warehouse/qimed_silver_completa.duckdb"
    conn = duckdb.connect(db_path, read_only=True)
    
    df_saude = conn.execute("SELECT COUNT(*) AS total FROM dim_operadoras_saude WHERE razao_social LIKE '%SA\u00daDE%'").df()
    df_benef = conn.execute("SELECT COUNT(*) AS total FROM dim_operadoras_saude WHERE razao_social LIKE '%BENEF\u00cdCIOS%'").df()
    df_assoc = conn.execute("SELECT COUNT(*) AS total FROM dim_operadoras_saude WHERE razao_social LIKE '%ASSOCIA\u00c7\u00c3O%'").df()
    
    conn.close()

    assert df_saude["total"][0] > 100, f"Esperado mais de 100 operadoras contendo 'SAUDE', obteve {df_saude['total'][0]}"
    assert df_benef["total"][0] > 50, f"Esperado mais de 50 operadoras contendo 'BENEFICIOS', obteve {df_benef['total'][0]}"
    assert df_assoc["total"][0] > 10, f"Esperado mais de 10 operadoras contendo 'ASSOCIACAO', obteve {df_assoc['total'][0]}"


def test_catalogo_dados_utf8_integro():
    """Valida que o arquivo catalogo_dados.py nao contem '?' no lugar de acentos."""
    from src.metadata.catalogo_dados import CATALOGO_ENTIDADES
    for entidade, descricao in CATALOGO_ENTIDADES.items():
        assert "?" not in descricao, f"Descricao da entidade '{entidade}' contem '?' indicando erro de encoding: {descricao}"
        assert any(c in descricao for c in "áéíóúãõçâêôà"), f"Descricao de '{entidade}' deveria conter acentos validos em portugues."


def test_ans_collector_parse_operadoras_mojibake_e_modalidade_nula():
    """Valida que _parse_operadoras sanitiza 100% de entradas com mojibake e preenche modalidades nulas/vazias."""
    import pandas as pd
    import numpy as np

    collector = AnsCollector(modalidade="operadoras")
    raw_df = pd.DataFrame([
        {"Registro_ANS": "000001", "Razao_Social": "SAÃDE BRASIL ASSISTÃŠNCIA MÃ‰DICA LTDA", "Modalidade": None, "UF": "SP"},
        {"Registro_ANS": "000002", "Razao_Social": "ASSOCIAÃ‡ÃƒO DOS FUNCIONÃ\x81RIOS", "Modalidade": "", "UF": "RJ"},
        {"Registro_ANS": "000003", "Razao_Social": "FUNDAÃ‡ÃƒO DE SAÃšDE E BENEFÃ\x8dCIOS", "Modalidade": "nan", "UF": "MG"},
        {"Registro_ANS": "000004", "Razao_Social": "BRADESCO SAÚDE S.A.", "Modalidade": "Seguradora Especializada em Saúde", "UF": "SP"},
        {"Registro_ANS": "000005", "Razao_Social": "UNIMED COOPERATIVA DE TRABALHO MÃ‰DICO", "Modalidade": "None", "UF": "BA"},
    ])

    parsed = collector.parse(raw_df)

    # 1. Nenhuma string com mojibake
    for r in parsed["razao_social"]:
        for mojibake_seq in ("Ã\x8d", "Ã\x93", "Ã\x9a", "Ã\x87", "Ã\x83", "ÃŠ", "Ãš", "Ã‰", "Ã“", "Ã‡", "Ãƒ", "SAÃDE", "â€", "Â"):
            assert mojibake_seq not in r, f"Mojibake '{mojibake_seq}' remanescente na razão social: {r}"
    
    assert "SAÚDE BRASIL ASSISTÊNCIA MÉDICA LTDA" in parsed["razao_social"].values
    assert "ASSOCIAÇÃO DOS FUNCIONÁRIOS" in parsed["razao_social"].values
    assert "FUNDAÇÃO DE SAÚDE E BENEFÍCIOS" in parsed["razao_social"].values
    assert "UNIMED COOPERATIVA DE TRABALHO MÉDICO" in parsed["razao_social"].values

    # 2. Modalidade sempre preenchida com "NÃO INFORMADA" quando nula/vazia
    assert (parsed["modalidade"] == "NÃO INFORMADA").sum() == 4
    assert parsed.loc[parsed["cd_operadora"] == "000004", "modalidade"].values[0] == "Seguradora Especializada em Saúde"


def test_dim_operadoras_saude_strict_sql_checks():
    """Valida os critérios de aceitação estritos em SQL sobre dim_operadoras_saude."""
    db_path = "warehouse/qimed_silver_completa.duckdb"
    assert os.path.exists(db_path), f"Arquivo {db_path} nao encontrado."

    conn = duckdb.connect(db_path, read_only=True)
    
    cnt_mojibake = conn.execute("""
        SELECT COUNT(*) 
        FROM dim_operadoras_saude 
        WHERE razao_social LIKE '%Ã\x8d%' 
           OR razao_social LIKE '%Ã\x93%' 
           OR razao_social LIKE '%Ã\x9a%' 
           OR razao_social LIKE '%Ã\x87%' 
           OR razao_social LIKE '%Ã\x83%' 
           OR razao_social LIKE '%ÃŠ%' 
           OR razao_social LIKE '%Ãš%' 
           OR razao_social LIKE '%Ã‰%' 
           OR razao_social LIKE '%Ã“%' 
           OR razao_social LIKE '%Ã‡%' 
           OR razao_social LIKE '%Ãƒ%' 
           OR razao_social LIKE '%Ãº%' 
           OR razao_social LIKE '%Ã§%' 
           OR razao_social LIKE '%Ã£%' 
           OR razao_social LIKE '%Ã©%' 
           OR razao_social LIKE '%Ã¡%' 
           OR razao_social LIKE '%Ãª%' 
           OR razao_social LIKE '%Ã­%' 
           OR razao_social LIKE '%Ã³%' 
           OR razao_social LIKE '%Ãµ%' 
           OR razao_social LIKE '%Ã´%' 
           OR razao_social LIKE '%Ã‚%' 
           OR razao_social LIKE '%Â %'
           OR razao_social LIKE '%Â°%'
           OR razao_social LIKE '%Â§%'
           OR razao_social LIKE '%â€%'
           OR razao_social LIKE '%SAÃDE%'
    """).fetchone()[0]
    
    cnt_modalidade_nula = conn.execute("SELECT COUNT(*) FROM dim_operadoras_saude WHERE modalidade_operadora IS NULL OR modalidade_operadora = '' OR modalidade_operadora = 'nan'").fetchone()[0]
    
    conn.close()

    assert cnt_mojibake == 0, f"Esperado 0 registros com mojibake, encontrado {cnt_mojibake}."
    assert cnt_modalidade_nula == 0, f"Esperado 0 registros com modalidade nula, encontrado {cnt_modalidade_nula}."
