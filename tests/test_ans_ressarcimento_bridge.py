"""
Testes Automatizados de Bridge MPI e Integridade Cont?bil em fct_ressarcimento_sus.
"""
import os
import duckdb
import pytest

DB_PATH = "warehouse/qimed_silver_completa.duckdb"


def test_bridge_mpi_ressarcimento_com_dim_paciente():
    """Validar que 100% dos pseud?nimos da fct_ressarcimento_sus encontram correspond?ncia na dim_paciente."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("""
        SELECT 
            COUNT(r.identificador_cobranca_abi) AS total_cobrancas,
            COUNT(p.pseudonimo_paciente) AS total_matches
        FROM fct_ressarcimento_sus r
        LEFT JOIN dim_paciente p ON r.pseudonimo_paciente = p.pseudonimo_paciente
    """).fetchone()
    conn.close()

    total_cobrancas, total_matches = res
    assert total_cobrancas > 0, "fct_ressarcimento_sus n?o pode estar vazia."
    assert total_cobrancas == total_matches, f"Nem todos os pacientes do ressarcimento casaram com a dim_paciente: {total_matches}/{total_cobrancas}"


def test_zeramento_valor_recolhido_em_recurso():
    """Validar que cobran?as em recurso possuem valor_recolhido_brl == 0.0 (exigibilidade suspensa)."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("""
        SELECT 
            COUNT(*) AS total_invalidos,
            COALESCE(SUM(valor_recolhido_brl), 0.0) AS soma_recolhido
        FROM fct_ressarcimento_sus
        WHERE UPPER(TRIM(CAST(situacao_cobranca AS VARCHAR))) LIKE '%RECURSO%'
          AND valor_recolhido_brl > 0.0
    """).fetchone()
    conn.close()

    total_invalidos, soma_recolhido = res
    assert total_invalidos == 0, f"Existem {total_invalidos} cobran?as em recurso com valor recolhido > 0 (R$ {soma_recolhido:,.2f})."


def test_zeramento_valor_recolhido_impugnado():
    """Validar que cobran?as impugnadas possuem valor_recolhido_brl == 0.0."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("""
        SELECT 
            COUNT(*) AS total_invalidos,
            COALESCE(SUM(valor_recolhido_brl), 0.0) AS soma_recolhido
        FROM fct_ressarcimento_sus
        WHERE UPPER(TRIM(CAST(situacao_cobranca AS VARCHAR))) LIKE '%IMPUGNAD%'
          AND valor_recolhido_brl > 0.0
    """).fetchone()
    conn.close()

    total_invalidos, soma_recolhido = res
    assert total_invalidos == 0, f"Existem {total_invalidos} cobran?as impugnadas com valor recolhido > 0 (R$ {soma_recolhido:,.2f})."


def test_preservacao_volumetria_cobrancas():
    """Validar que a tabela mant?m exatamente 340.918 linhas."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    total_linhas = conn.execute("SELECT COUNT(*) FROM fct_ressarcimento_sus").fetchone()[0]
    conn.close()

    assert total_linhas == 340918, f"Esperavam-se 340.918 linhas em fct_ressarcimento_sus, mas foram encontradas {total_linhas}."
