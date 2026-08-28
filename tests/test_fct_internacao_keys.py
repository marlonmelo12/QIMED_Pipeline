"""
Testes Automatizados para fct_internacao:
1. Unicidade de id_internacao_hospitalar (PK a n?vel de linha)
2. Tipagem de numero_aih_proxima como VARCHAR
3. Agrupamento de id_episodio_internacao em AIHs de m?ltiplos faturamentos
4. Preserva??o de volumetria integral (1.250.737 linhas)
"""
import os
import duckdb
import pytest

DB_PATH = "warehouse/qimed_silver_completa.duckdb"


def test_unicidade_id_internacao_hospitalar():
    """Valida que COUNT(DISTINCT id_internacao_hospitalar) == COUNT(*) no banco DuckDB."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    res = conn.execute("""
        SELECT 
            COUNT(*) AS total_linhas,
            COUNT(DISTINCT id_internacao_hospitalar) AS total_pks,
            COUNT(id_internacao_hospitalar) AS total_nao_nulos
        FROM fct_internacao
    """).fetchone()
    conn.close()

    total_linhas, total_pks, total_nao_nulos = res
    assert total_linhas > 0, "A tabela fct_internacao n?o pode estar vazia."
    assert total_linhas == total_pks, f"id_internacao_hospitalar cont?m duplicatas: {total_linhas} linhas vs {total_pks} PKs ?nicas."
    assert total_linhas == total_nao_nulos, "id_internacao_hospitalar possui valores nulos."


def test_tipagem_numero_aih_proxima_varchar():
    """Valida que a coluna numero_aih_proxima ? do tipo VARCHAR no schema."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    df_desc = conn.execute("DESCRIBE fct_internacao").df()
    conn.close()

    col_info = df_desc[df_desc["column_name"] == "numero_aih_proxima"]
    assert not col_info.empty, "Coluna 'numero_aih_proxima' n?o encontrada na tabela fct_internacao."
    col_type = col_info["column_type"].iloc[0].upper()
    assert "VARCHAR" in col_type or "STRING" in col_type, f"Tipo de numero_aih_proxima deveria ser VARCHAR, mas ? {col_type}."


def test_agrupamento_id_episodio_internacao():
    """
    Valida que as 444 AIHs com m?ltiplos faturamentos compartilham o mesmo id_episodio_internacao 
    para o mesmo paciente e data de admiss?o.
    """
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # AIHs que possuem mais de 1 linha de faturamento
    df_multi = conn.execute("""
        WITH aihs_repetidas AS (
            SELECT numero_aih
            FROM fct_internacao
            GROUP BY numero_aih
            HAVING COUNT(*) > 1
        )
        SELECT 
            f.numero_aih,
            f.pseudonimo_paciente,
            f.data_internacao,
            COUNT(*) AS total_faturamentos,
            COUNT(DISTINCT f.id_episodio_internacao) AS total_episodios
        FROM fct_internacao f
        JOIN aihs_repetidas r ON f.numero_aih = r.numero_aih
        GROUP BY f.numero_aih, f.pseudonimo_paciente, f.data_internacao
    """).df()
    
    total_aihs_distintas = conn.execute("""
        SELECT COUNT(DISTINCT numero_aih)
        FROM fct_internacao
        GROUP BY numero_aih
        HAVING COUNT(*) > 1
    """).fetchall()
    conn.close()

    assert len(total_aihs_distintas) == 444, f"Esperavam-se 444 AIHs de m?ltiplos faturamentos, mas foram encontradas {len(total_aihs_distintas)}."
    
    # Para o mesmo paciente e admiss?o, o epis?dio deve ser estritamente ?nico (total_episodios == 1)
    assert (df_multi["total_episodios"] == 1).all(), "Existem faturamentos de mesma admiss?o com id_episodio_internacao divergente."


def test_preservacao_volumetria_sem_delecao():
    """Valida que o total de linhas permanece exatamente igual a 1.250.737 (sem DISTINCT ON)."""
    assert os.path.exists(DB_PATH), f"Banco {DB_PATH} n?o encontrado."
    conn = duckdb.connect(DB_PATH, read_only=True)
    total_linhas = conn.execute("SELECT COUNT(*) FROM fct_internacao").fetchone()[0]
    conn.close()

    assert total_linhas == 1250737, f"Esperavam-se 1.250.737 linhas em fct_internacao, mas foram encontradas {total_linhas}."
