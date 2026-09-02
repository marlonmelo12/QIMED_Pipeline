"""
Suíte de Testes de Produção: dim_estabelecimento Gold.
Cobre rigorosamente os 12 cenários obrigatórios de qualidade de dados, integridade dimensional,
resolução territorial oficial via IBGE e proibição de dados sintéticos.
"""
import pytest
import duckdb
import os
import pandas as pd
from src.gold.models.dim_estabelecimento import build_dim_estabelecimento


@pytest.fixture
def test_db():
    """Cria uma conexão DuckDB em memória com fixture para testes."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def ibge_catalog_file(tmp_path):
    """Cria um arquivo parquet de catálogo IBGE com amostra de municípios reais."""
    ibge_data = [
        {"municipality_code_6": "261160", "municipality_code_7": "2611606", "municipality_name": "Recife", "uf_sigla": "PE", "estado_nome": "Pernambuco", "regiao": "Nordeste"},
        {"municipality_code_6": "260410", "municipality_code_7": "2604106", "municipality_name": "Caruaru", "uf_sigla": "PE", "estado_nome": "Pernambuco", "regiao": "Nordeste"},
        {"municipality_code_6": "355030", "municipality_code_7": "3550308", "municipality_name": "São Paulo", "uf_sigla": "SP", "estado_nome": "São Paulo", "regiao": "Sudeste"},
        {"municipality_code_6": "330455", "municipality_code_7": "3304557", "municipality_name": "Rio de Janeiro", "uf_sigla": "RJ", "estado_nome": "Rio de Janeiro", "regiao": "Sudeste"},
        {"municipality_code_6": "230440", "municipality_code_7": "2304400", "municipality_name": "Fortaleza", "uf_sigla": "CE", "estado_nome": "Ceará", "regiao": "Nordeste"},
    ]
    df = pd.DataFrame(ibge_data)
    cat_path = str(tmp_path / "test_dim_municipios_ibge.parquet")
    df.to_parquet(cat_path, index=False)
    return cat_path


@pytest.fixture
def cnes_catalog_file(tmp_path):
    """Cria um arquivo parquet de catálogo CNES oficial para testes."""
    cnes_data = [
        {"codigo_estabelecimento_cnes": "2345678", "nome_fantasia": "HOSPITAL REAL PORTUGUES", "razao_social": "REAL SOCIEDADE DE BENEFICENCIA", "tipo_unidade": "HOSPITAL GERAL"},
        {"codigo_estabelecimento_cnes": "9876543", "nome_fantasia": "HOSPITAL DAS CLINICAS FMUSP", "razao_social": "HOSPITAL DAS CLINICAS DA FACULDADE DE MEDICINA DA USP", "tipo_unidade": "HOSPITAL DE ENSINO"},
    ]
    df = pd.DataFrame(cnes_data)
    cat_path = str(tmp_path / "test_dim_cnes_datasus.parquet")
    df.to_parquet(cat_path, index=False)
    return cat_path


def _setup_silver_tables(conn: duckdb.DuckDBPyConnection):
    """Cria e popula fct_internacao para testes."""
    conn.execute("""
    CREATE TABLE fct_internacao (
        numero_aih VARCHAR,
        codigo_estabelecimento_cnes VARCHAR,
        codigo_municipio_hospital VARCHAR,
        uf VARCHAR
    );
    INSERT INTO fct_internacao VALUES
        ('AIH001', '2345678', '261160', 'PE'),
        ('AIH002', '2345678', '261160', 'PE'),
        ('AIH003', '9876543', '355030', 'SP'),
        ('AIH004', '1122334', '999999', 'XX'); -- Município inexistente no IBGE
    """)


# ==============================================================================
# OS 12 CENÁRIOS OBRIGATÓRIOS
# ==============================================================================

def test_1_cnes_real_preservado(test_db, ibge_catalog_file):
    """Teste 1: CNES real presente na fonte deve preservar exatamente o identificador."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    cnes_list = [r[0] for r in test_db.execute("SELECT codigo_estabelecimento_cnes FROM dim_estabelecimento ORDER BY 1;").fetchall()]
    assert "2345678" in cnes_list
    assert "9876543" in cnes_list
    assert "1122334" in cnes_list
    assert len(cnes_list) == 3


def test_2_nome_oficial_ou_null(test_db, ibge_catalog_file):
    """Teste 2: Se não houver fonte cadastral, nome_fantasia deve ser NULL (nunca inventado)."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    nomes = [r[0] for r in test_db.execute("SELECT nome_fantasia FROM dim_estabelecimento;").fetchall()]
    assert all(n is None for n in nomes), "nome_fantasia deve ser NULL quando não fornecido por cadastro oficial"


def test_3_razao_social_ou_null(test_db, ibge_catalog_file):
    """Teste 3: razao_social deve ser preservada se vier da fonte cadastral, ou NULL."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    razoes = [r[0] for r in test_db.execute("SELECT razao_social FROM dim_estabelecimento;").fetchall()]
    assert all(r is None for r in razoes)


def test_4_tipo_unidade_ou_null(test_db, ibge_catalog_file):
    """Teste 4: tipo_unidade deve vir de cadastro oficial ou ser NULL."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    tipos = [r[0] for r in test_db.execute("SELECT tipo_unidade FROM dim_estabelecimento;").fetchall()]
    assert all(t is None for t in tipos)


def test_5_municipio_resolucao_ibge(test_db, ibge_catalog_file):
    """Teste 5: Validar codigo_municipio -> IBGE -> município correto."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    recife = test_db.execute("SELECT municipio, uf, estado_nome, regiao FROM dim_estabelecimento WHERE codigo_estabelecimento_cnes = '2345678';").fetchone()
    assert recife[0] == "Recife"
    assert recife[1] == "PE"
    assert recife[2] == "Pernambuco"
    assert recife[3] == "Nordeste"

    sp = test_db.execute("SELECT municipio, uf, estado_nome, regiao FROM dim_estabelecimento WHERE codigo_estabelecimento_cnes = '9876543';").fetchone()
    assert sp[0] == "São Paulo"
    assert sp[1] == "SP"


def test_6_municipio_inexistente_sem_fallback_sintetico(test_db, ibge_catalog_file):
    """Teste 6: Código IBGE inexistente não deve produzir 'Polo Regional ...'. Deve ser NULL."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    inexistente = test_db.execute("SELECT municipio, uf FROM dim_estabelecimento WHERE codigo_estabelecimento_cnes = '1122334';").fetchone()
    assert inexistente[0] is None, f"Município inexistente deve ser NULL, mas foi: {inexistente[0]}"
    assert inexistente[1] == "XX"


def test_7_cnes_unicidade_do_grain(test_db, ibge_catalog_file):
    """Teste 7: Garantir exatamente uma linha por CNES (COUNT(*) == COUNT(DISTINCT CNES))."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    total = test_db.execute("SELECT COUNT(*) FROM dim_estabelecimento;").fetchone()[0]
    distinct_cnes = test_db.execute("SELECT COUNT(DISTINCT codigo_estabelecimento_cnes) FROM dim_estabelecimento;").fetchone()[0]
    assert total == distinct_cnes
    assert total == 3


def test_8_cnes_inconsistente_auditoria(test_db, ibge_catalog_file):
    """Teste 8: Detectar CNES associado a múltiplos municípios/UFs sem corromper o grain."""
    conn = test_db
    conn.execute("""
    CREATE TABLE fct_internacao (
        codigo_estabelecimento_cnes VARCHAR,
        codigo_municipio_hospital VARCHAR,
        uf VARCHAR
    );
    INSERT INTO fct_internacao VALUES
        ('CNES_INCONSISTENTE', '261160', 'PE'),
        ('CNES_INCONSISTENTE', '355030', 'SP'); -- Mesmo CNES em dois municípios
    """)

    build_dim_estabelecimento(conn, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")
    total = conn.execute("SELECT COUNT(*) FROM dim_estabelecimento;").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT codigo_estabelecimento_cnes) FROM dim_estabelecimento;").fetchone()[0]
    assert total == 1
    assert distinct == 1


def test_9_ausencia_da_fonte_falha_explicita(test_db, ibge_catalog_file):
    """Teste 9: Se a fonte fct_internacao não existir, falhar explicitamente sem criar Gold vazia."""
    with pytest.raises(ValueError, match="Required Silver source not found"):
        build_dim_estabelecimento(test_db, fct_table="fct_internacao_inexistente", ibge_catalog_path=ibge_catalog_file)


def test_10_ausencia_de_coluna_obrigatoria(test_db, ibge_catalog_file):
    """Teste 10: Se uma coluna obrigatória faltar, falhar com mensagem clara."""
    test_db.execute("""
    CREATE TABLE fct_internacao (
        codigo_estabelecimento_cnes VARCHAR
        -- Faltam codigo_municipio_hospital e uf
    );
    """)
    with pytest.raises(ValueError, match="Required column.*missing"):
        build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file)


def test_11_idempotencia(test_db, ibge_catalog_file):
    """Teste 11: Executar duas vezes deve produzir o mesmo conteúdo lógico."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")
    res1 = test_db.execute("SELECT codigo_estabelecimento_cnes, municipio, uf FROM dim_estabelecimento ORDER BY 1;").fetchall()

    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")
    res2 = test_db.execute("SELECT codigo_estabelecimento_cnes, municipio, uf FROM dim_estabelecimento ORDER BY 1;").fetchall()

    assert res1 == res2


def test_12_proibicao_de_dados_sinteticos(test_db, ibge_catalog_file):
    """Teste 12: Garantir que nenhum registro possua padrões artificiais."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path="inexistente.parquet")

    sinteticos = test_db.execute("""
        SELECT *
        FROM dim_estabelecimento
        WHERE nome_fantasia ILIKE '%Polo Regional%'
           OR nome_fantasia ILIKE '%Hospital Regional de%'
           OR nome_fantasia ILIKE '%Hospital Infantil de%'
           OR municipio ILIKE '%Polo Regional%';
    """).fetchall()

    assert len(sinteticos) == 0, f"Encontrados dados sintéticos proibidos: {sinteticos}"


def test_enriquecimento_com_catalogo_cnes_oficial(test_db, ibge_catalog_file, cnes_catalog_file):
    """Teste 13: Valida o enriquecimento oficial com nomes reais do catálogo CNES."""
    _setup_silver_tables(test_db)
    build_dim_estabelecimento(test_db, ibge_catalog_path=ibge_catalog_file, cnes_catalog_path=cnes_catalog_file)

    row = test_db.execute("SELECT nome_fantasia, razao_social, tipo_unidade FROM dim_estabelecimento WHERE codigo_estabelecimento_cnes = '2345678';").fetchone()
    assert row[0] == "HOSPITAL REAL PORTUGUES"
    assert row[1] == "REAL SOCIEDADE DE BENEFICENCIA"
    assert row[2] == "HOSPITAL GERAL"
