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

DICIONARIO_PATH = "docs/dicionario_silver_duckdb.md"


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


def test_dicionario_tagging_completo():
    """Validar que todas as entidades do dicionário possuem tag formal de ciclo de vida."""
    assert os.path.exists(DICIONARIO_PATH), f"Dicionário {DICIONARIO_PATH} não encontrado."
    with open(DICIONARIO_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    headers = re.findall(r"##\s+`?([a-zA-Z0-9_]+)`?\s+\[STATUS:\s+([^\]]+)\]", content)
    assert len(headers) >= 10, f"Foram encontradas apenas {len(headers)} entidades com tags no dicionário."

    valid_tags = {
        "ENTREGUE / SILVER ATIVA",
        "ENTREGUE / GOLD DW ATIVA",
        "EM IMPLEMENTAÇÃO / ROADMAP EXPANSÃO"
    }

    for entidade, status in headers:
        assert status in valid_tags, f"Entidade '{entidade}' possui tag inválida: '{status}'"

    entidades_encontradas = [h[0] for h in headers]
    assert "fct_internacao" in entidades_encontradas
    assert "fct_atendimentos_ambulatoriais" in entidades_encontradas
    assert "fct_ressarcimento_sus" in entidades_encontradas
    assert "fct_glosas_hospitalares" in entidades_encontradas
    assert "fct_glosas_tiss" in entidades_encontradas
    assert "aud_alertas_anomalias" in entidades_encontradas
