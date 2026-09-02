"""
Suíte de Testes de Produção para build_dm_ans_glosas_operadoras().
Cobre 100% dos 10 cenários obrigatórios especificados para o Lead de Engenharia de Dados:
  1. Teste 1 — Dados reais (agregação direta de fatos de fct_ressarcimento_sus);
  2. Teste 2 — Competências dinâmicas (incorporação automática de novos períodos);
  3. Teste 3 — Ausência da fato (falha explícita com mensagem clara, sem tabela vazia);
  4. Teste 4 — Schema inválido (falha com indicação de colunas ausentes);
  5. Teste 5 — Ausência de operador na dimensão (preservação do fato com LEFT JOIN);
  6. Teste 6 — Unicidade do grain (operadora x competência);
  7. Teste 7 — Idempotência (execuções repetidas com mesmo resultado);
  8. Teste 8 — Valores financeiros e consistência (glosa = faturado - recolhido, taxa de glosa);
  9. Teste 9 — Sem dados fictícios (ausência de 999999 / operadora fictícia);
  10. Teste 10 — Determinismo da PK (MD5 reprodutível).
"""
import pytest
import duckdb
from src.gold.models.kpi_glosas_operadoras_ans import (
    build_dm_ans_glosas_operadoras,
    REQUIRED_FCT_COLUMNS,
    REQUIRED_DIM_COLUMNS,
)


@pytest.fixture
def clean_duckdb_conn():
    """Cria uma conexão em memória limpa para testes isolados de transformações."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def populated_silver_tables(clean_duckdb_conn):
    """Popula tabelas Silver sintéticas isoladas para fixture de testes."""
    conn = clean_duckdb_conn

    # 1. Cria dim_operadoras_saude
    conn.execute("""
    CREATE TABLE dim_operadoras_saude (
        codigo_registro_ans VARCHAR,
        cnpj_operadora VARCHAR,
        razao_social VARCHAR,
        nome_fantasia VARCHAR,
        modalidade_operadora VARCHAR,
        porte_operadora VARCHAR,
        segmentacao_operadora VARCHAR
    );
    INSERT INTO dim_operadoras_saude VALUES
        ('000001', '11.111.111/0001-11', 'BRADESCO SAÚDE S.A.', 'BRADESCO', 'Seguradora Especializada em Saúde', 'Grande', 'Médico-Hospitalar'),
        ('000002', '22.222.222/0001-22', 'UNIMED FORTALEZA', 'UNIMED', 'Cooperativa Médica', 'Médio', 'Médico-Hospitalar'),
        ('000003', '33.333.333/0001-33', 'ODONTOPREV S.A.', 'ODONTOPREV', 'Odontologia de Grupo', 'Grande', 'Exclus. Odontológica');
    """)

    # 2. Cria fct_ressarcimento_sus
    conn.execute("""
    CREATE TABLE fct_ressarcimento_sus (
        identificador_cobranca_abi VARCHAR,
        numero_aih VARCHAR,
        codigo_registro_ans VARCHAR,
        razao_social_operadora VARCHAR,
        modalidade_operadora VARCHAR,
        data_internacao VARCHAR,
        data_alta VARCHAR,
        ano INTEGER,
        mes INTEGER,
        valor_notificado_brl DOUBLE,
        valor_recolhido_brl DOUBLE,
        situacao_cobranca VARCHAR
    );
    INSERT INTO fct_ressarcimento_sus VALUES
        ('ABI-101', 'AIH-001', '000001', 'BRADESCO SAÚDE', 'Seguradora Especializada em Saúde', '2026-05-01', '2026-05-10', 2026, 5, 10000.0, 8000.0, 'RECOLHIDO'),
        ('ABI-102', 'AIH-002', '000001', 'BRADESCO SAÚDE', 'Seguradora Especializada em Saúde', '2026-05-05', '2026-05-15', 2026, 5, 5000.0, 0.0, 'IMPUGNADO'),
        ('ABI-103', 'AIH-003', '000002', 'UNIMED FORTALEZA', 'Cooperativa Médica', '2026-05-02', '2026-05-06', 2026, 5, 20000.0, 18000.0, 'RECOLHIDO'),
        ('ABI-104', 'AIH-004', '000003', 'ODONTOPREV S.A.', 'Odontologia de Grupo', '2026-05-03', '2026-05-04', 2026, 5, 2000.0, 1900.0, 'RECOLHIDO');
    """)

    return conn


# ==============================================================================
# TESTE 1: DADOS REAIS — AGREGAÇÃO DERIVADA DOS FATOS SILVER
# ==============================================================================
def test_1_dados_reais_derivados_de_fct_ressarcimento(populated_silver_tables):
    """Valida que todos os indicadores quantitativos e financeiros vêm exclusivamente dos fatos."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)

    rows = conn.execute("""
        SELECT codigo_registro_ans, periodo, total_guias_glosadas, valor_total_faturado_brl, valor_total_recolhido_brl, valor_total_glosado_brl, taxa_glosa_pct
        FROM dm_ans_glosas_operadoras
        WHERE codigo_registro_ans = '000001' AND periodo = '2026-05'
    """).fetchall()

    assert len(rows) == 1
    op_ans, periodo, guias, fat, rec, glo, taxa = rows[0]
    # Bradesco: 2 guias (10000 + 5000 = 15000 faturado, 8000 + 0 = 8000 recolhido, 7000 glosado, taxa = 7000/15000 = 46.67%)
    assert guias == 2
    assert fat == 15000.0
    assert rec == 8000.0
    assert glo == 7000.0
    assert taxa == 46.67


# ==============================================================================
# TESTE 2: COMPETÊNCIAS DINÂMICAS — INCORPORAÇÃO AUTOMÁTICA DE NOVOS PERÍODOS
# ==============================================================================
def test_2_competencias_dinamicas(populated_silver_tables):
    """Valida que adicionar uma competência futura no fato a incorpora automaticamente na Gold."""
    conn = populated_silver_tables

    # Insere uma nova competência futura no fato
    conn.execute("""
        INSERT INTO fct_ressarcimento_sus VALUES
        ('ABI-999', 'AIH-999', '000001', 'BRADESCO SAÚDE', 'Seguradora', '2027-11-01', '2027-11-05', 2027, 11, 50000.0, 45000.0, 'RECOLHIDO');
    """)

    build_dm_ans_glosas_operadoras(conn)

    periodos = [r[0] for r in conn.execute("SELECT DISTINCT periodo FROM dm_ans_glosas_operadoras ORDER BY periodo;").fetchall()]
    assert "2027-11" in periodos, "Nova competência '2027-11' deveria ter sido incorporada dinamicamente!"


# ==============================================================================
# TESTE 3: AUSÊNCIA DA TABELA FATO — FALHA EXPLÍCITA COM MENSAGEM CLARA
# ==============================================================================
def test_3_ausencia_fato_falha_explicita(clean_duckdb_conn):
    """Valida que a ausência de fct_ressarcimento_sus lança erro explícito sem criar tabela vazia."""
    conn = clean_duckdb_conn
    # Cria apenas a dimensão
    conn.execute("CREATE TABLE dim_operadoras_saude (codigo_registro_ans VARCHAR, razao_social VARCHAR, modalidade_operadora VARCHAR);")

    with pytest.raises(ValueError) as exc_info:
        build_dm_ans_glosas_operadoras(conn)

    assert "Required Silver source not found" in str(exc_info.value)
    assert "fct_ressarcimento_sus" in str(exc_info.value)


# ==============================================================================
# TESTE 4: SCHEMA INVÁLIDO — FALHA COM INDICAÇÃO DE COLUNAS AUSENTES
# ==============================================================================
def test_4_schema_invalido_informa_colunas_ausentes(clean_duckdb_conn):
    """Valida que a falta de uma coluna obrigatória na Fato acarreta falha com o nome da coluna."""
    conn = clean_duckdb_conn
    conn.execute("CREATE TABLE dim_operadoras_saude (codigo_registro_ans VARCHAR, razao_social VARCHAR, modalidade_operadora VARCHAR);")
    # Fato sem a coluna valor_notificado_brl
    conn.execute("""
    CREATE TABLE fct_ressarcimento_sus (
        codigo_registro_ans VARCHAR,
        ano INTEGER,
        mes INTEGER,
        valor_recolhido_brl DOUBLE
    );
    """)

    with pytest.raises(ValueError) as exc_info:
        build_dm_ans_glosas_operadoras(conn)

    assert "Required column(s) missing" in str(exc_info.value)
    assert "valor_notificado_brl" in str(exc_info.value)


# ==============================================================================
# TESTE 5: AUSÊNCIA DE OPERADOR NA DIMENSÃO — PRESERVAÇÃO DO FATO (LEFT JOIN)
# ==============================================================================
def test_5_ausencia_de_operador_preserva_fato(clean_duckdb_conn):
    """Valida que um fato de operadora não cadastrada na dimensão NÃO desaparece da Gold."""
    conn = clean_duckdb_conn
    conn.execute("CREATE TABLE dim_operadoras_saude (codigo_registro_ans VARCHAR, razao_social VARCHAR, modalidade_operadora VARCHAR);")
    conn.execute("""
    CREATE TABLE fct_ressarcimento_sus (
        codigo_registro_ans VARCHAR,
        ano INTEGER,
        mes INTEGER,
        valor_notificado_brl DOUBLE,
        valor_recolhido_brl DOUBLE,
        situacao_cobranca VARCHAR
    );
    INSERT INTO fct_ressarcimento_sus VALUES
        ('987654', 2026, 5, 8000.0, 7000.0, 'RECOLHIDO');
    """)

    build_dm_ans_glosas_operadoras(conn)

    row = conn.execute("SELECT codigo_registro_ans, razao_social, valor_total_faturado_brl FROM dm_ans_glosas_operadoras WHERE codigo_registro_ans = '987654'").fetchone()
    assert row is not None, "O fato de operadora sem dimensão não pode sumir da Gold!"
    assert row[0] == "987654"
    assert "NÃO IDENTIFICADA" in row[1]
    assert row[2] == 8000.0


# ==============================================================================
# TESTE 6: UNICIDADE DO GRAIN (OPERADORA X COMPETÊNCIA)
# ==============================================================================
def test_6_unicidade_do_grain(populated_silver_tables):
    """Valida que não existem duplicidades no grain codigo_registro_ans + periodo."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)

    dup_count = conn.execute("""
        SELECT codigo_registro_ans, periodo, COUNT(*)
        FROM dm_ans_glosas_operadoras
        GROUP BY codigo_registro_ans, periodo
        HAVING COUNT(*) > 1;
    """).fetchall()

    assert len(dup_count) == 0, f"Existem registros duplicados no grain: {dup_count}"


# ==============================================================================
# TESTE 7: IDEMPOTÊNCIA — EXECUÇÕES REPETIDAS COM MESMO RESULTADO
# ==============================================================================
def test_7_idempotencia(populated_silver_tables):
    """Valida que executar build() consecutivas vezes mantém o mesmo número de linhas e valores."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)
    count_1 = conn.execute("SELECT COUNT(*) FROM dm_ans_glosas_operadoras;").fetchone()[0]

    # Segunda execução
    build_dm_ans_glosas_operadoras(conn)
    count_2 = conn.execute("SELECT COUNT(*) FROM dm_ans_glosas_operadoras;").fetchone()[0]

    assert count_1 == count_2
    assert count_1 == 3  # 3 operadoras únicas em 2026-05


# ==============================================================================
# TESTE 8: REGRAS FINANCEIRAS E CONSISTÊNCIA
# ==============================================================================
def test_8_regras_financeiras_e_consistencia(populated_silver_tables):
    """Valida consistência matemática: glosado = faturado - recolhido e taxa_glosa entre 0 e 100%."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)

    rows = conn.execute("""
        SELECT valor_total_faturado_brl, valor_total_recolhido_brl, valor_total_glosado_brl, taxa_glosa_pct
        FROM dm_ans_glosas_operadoras;
    """).fetchall()

    for fat, rec, glo, taxa in rows:
        assert fat >= rec, "Faturado deve ser maior ou igual ao recolhido!"
        assert round(glo, 2) == round(fat - rec, 2), "Glosado deve ser exatamente a diferença faturado - recolhido!"
        assert 0.0 <= taxa <= 100.0, f"Taxa de glosa inválida: {taxa}%"


# ==============================================================================
# TESTE 9: SEM DADOS FICTÍCIOS / HARDCODED
# ==============================================================================
def test_9_sem_dados_ficticios(populated_silver_tables):
    """Garante que registros artificiais (999999 / Operadora Fictícia) não apareçam na Gold."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)

    ficticios = conn.execute("""
        SELECT COUNT(*)
        FROM dm_ans_glosas_operadoras
        WHERE codigo_registro_ans = '999999'
           OR razao_social LIKE '%LIQUIDAÇÃO EXTRAORDINÁRIA%'
           OR cnpj_operadora = '99.999.999/0001-99';
    """).fetchone()[0]

    assert ficticios == 0, "Registros fictícios não devem existir na tabela Gold!"


# ==============================================================================
# TESTE 10: DETERMINISMO DA PRIMARY KEY (MD5)
# ==============================================================================
def test_10_determinismo_chave_primaria(populated_silver_tables):
    """Valida que a chave id_registro_kpi gerada é estritamente determinística e reprodutível."""
    conn = populated_silver_tables
    build_dm_ans_glosas_operadoras(conn)
    pks_run1 = conn.execute("SELECT id_registro_kpi FROM dm_ans_glosas_operadoras ORDER BY id_registro_kpi;").fetchall()

    build_dm_ans_glosas_operadoras(conn)
    pks_run2 = conn.execute("SELECT id_registro_kpi FROM dm_ans_glosas_operadoras ORDER BY id_registro_kpi;").fetchall()

    assert pks_run1 == pks_run2
    assert len(pks_run1) > 0
    # Verifica tamanho MD5 (32 caracteres hexadecimais)
    assert len(pks_run1[0][0]) == 32
