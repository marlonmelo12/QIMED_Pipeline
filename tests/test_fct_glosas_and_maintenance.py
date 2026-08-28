"""
Testes Automatizados para Ingestão SIH-RJ/ER, Conector TISS, Compactação DuckDB e Dicionário de Dados.
"""
import os
import re
import duckdb
import pytest

from src.collectors.datasus_collector import DatasusCollector
from src.collectors.tiss_collector import TissCollector, TABELA_38_TUSS_GLOSAS
from src.dw.maintenance import otimizar_storage_duckdb


def test_datasus_collector_sih_rj_url():
    """Validar montagem correta da URL FTP para SIH-RJ."""
    collector = DatasusCollector(subsystem="SIH-RJ", uf="SP", year=2026, month=5)
    ftp_path = collector._get_ftp_path()
    assert "ftp.datasus.gov.br" in ftp_path
    assert "/SIHSUS/200801_/Dados" in ftp_path
    assert "RJSP2605.dbc" in ftp_path


def test_datasus_collector_sih_er_url():
    """Validar montagem correta da URL FTP para SIH-ER."""
    collector = DatasusCollector(subsystem="SIH-ER", uf="RJ", year=2026, month=10)
    ftp_path = collector._get_ftp_path()
    assert "ftp.datasus.gov.br" in ftp_path
    assert "/SIHSUS/200801_/Dados" in ftp_path
    assert "ERRJ2610.dbc" in ftp_path


def test_tiss_collector_tabela38_parsing():
    """Validar conector TISS e enriquecimento com Tabela 38 da ANS."""
    collector = TissCollector(registro_ans="345678", ano=2026, mes=5)
    dados = collector.fetch()
    df = collector.parse(dados)

    assert not df.empty, "DataFrame TISS não pode ser vazio."
    assert "codigo_glosa_tuss" in df.columns
    assert "descricao_glosa_tuss" in df.columns
    assert "valor_glosado_brl" in df.columns

    glosas_presentes = df["codigo_glosa_tuss"].tolist()
    assert "2005" in glosas_presentes or "1001" in glosas_presentes
    for _, row in df.iterrows():
        cod = row["codigo_glosa_tuss"]
        if cod in TABELA_38_TUSS_GLOSAS:
            assert row["descricao_glosa_tuss"] == TABELA_38_TUSS_GLOSAS[cod]


def test_otimizacao_storage_duckdb(tmp_path):
    """Validar execução sem erros de CHECKPOINT e VACUUM no banco DuckDB."""
    test_db = os.path.join(tmp_path, "test_vacuum.duckdb")
    conn = duckdb.connect(test_db)
    conn.execute("CREATE TABLE teste AS SELECT range AS id, 'valor' AS txt FROM range(1000)")
    conn.close()

    assert os.path.exists(test_db)
    otimizar_storage_duckdb(test_db)
    
    conn2 = duckdb.connect(test_db)
    cnt = conn2.execute("SELECT COUNT(*) FROM teste").fetchone()[0]
    conn2.close()
    assert cnt == 1000


from src.metadata.catalogo_dados import CATALOGO_ENTIDADES


def test_catalogo_entidades_completo():
    """Validar que todas as entidades do catálogo de dados possuem descrição técnica formal."""
    assert len(CATALOGO_ENTIDADES) >= 10, f"Foram encontradas apenas {len(CATALOGO_ENTIDADES)} entidades no catálogo."

    entidades_esperadas = [
        "fct_internacao",
        "fct_atendimentos_ambulatoriais",
        "fct_ressarcimento_sus",
        "fct_glosas_hospitalares",
        "aud_alertas_anomalias",
        "dim_paciente",
        "dim_operadoras_saude",
        "dim_estabelecimento",
        "dim_tempo",
    ]

    for ent in entidades_esperadas:
        assert ent in CATALOGO_ENTIDADES, f"Entidade '{ent}' ausente do catálogo."
        assert len(CATALOGO_ENTIDADES[ent]) > 10, f"Descrição técnica de '{ent}' muito curta."
