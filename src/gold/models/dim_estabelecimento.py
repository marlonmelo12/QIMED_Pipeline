"""
Modelo Gold: dim_estabelecimento (Produção).

Materializa a dimensão de estabelecimentos de saúde (dim_estabelecimento) no DuckDB DW
baseada exclusivamente em fontes factuais reais da camada Silver, no catálogo
oficial de municípios do IBGE (5.570 municípios) e no catálogo oficial de estabelecimentos
do DATASUS / Ministério da Saúde (config/dim_cnes_datasus.parquet).

Princípios de Engenharia de Dados aplicados:
  1. Zero dados sintéticos: dados cadastrais extraídos diretamente das fontes governamentais oficiais;
  2. Resolução territorial oficial: cruzamento determinístico no DuckDB via catálogo oficial do IBGE;
  3. Enriquecimento cadastral oficial: cruzamento nativo com o catálogo oficial do CNES/DATASUS;
  4. Grain canônico e estrito: 1 registro = 1 estabelecimento CNES (codigo_estabelecimento_cnes);
  5. Validação estrita de contratos Silver (Fail Fast): falha explícita se fontes ou colunas obrigatórias estiverem ausentes;
  6. Detecção de inconsistências: auditoria de integridade para CNES com múltiplos municípios/UFs;
  7. Idempotência e processamento nativo em SQL DuckDB.
"""
import os
from typing import Optional, Set
import duckdb
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Contratos obrigatórios para a fonte de estabelecimentos
REQUIRED_FCT_COLUMNS: Set[str] = {
    "codigo_estabelecimento_cnes",
    "codigo_municipio_hospital",
    "uf",
}


def _get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> Set[str]:
    """Retorna o conjunto de nomes de colunas de uma tabela no DuckDB em minúsculas."""
    try:
        describe_rows = conn.execute(f"DESCRIBE {table_name};").fetchall()
        return {str(row[0]).lower() for row in describe_rows}
    except Exception as e:
        logger.error(f"[VALIDATION] Erro ao inspecionar schema da tabela '{table_name}': {e}")
        raise


def _validate_source_contracts(
    conn: duckdb.DuckDBPyConnection,
    fct_table: str,
    ibge_catalog_path: str,
) -> None:
    """
    Valida a existência e integridade de schema da fonte Silver e do catálogo IBGE.
    Falha imediatamente (Fail Fast) com ValueError explícito caso tabelas, arquivos ou colunas faltem.
    """
    existing_tables = {row[0].lower() for row in conn.execute("SHOW TABLES;").fetchall()}

    # 1. Validação de existência da tabela de fatos
    if fct_table.lower() not in existing_tables:
        msg = f"Required Silver source not found: '{fct_table}'"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    # 2. Validação de colunas obrigatórias
    fct_cols = _get_table_columns(conn, fct_table)
    missing_cols = REQUIRED_FCT_COLUMNS - fct_cols
    if missing_cols:
        msg = f"Required column(s) missing in Silver source '{fct_table}': {sorted(missing_cols)}"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    # 3. Validação de existência do catálogo oficial IBGE
    if not os.path.exists(ibge_catalog_path):
        msg = f"IBGE catalog not found at: '{ibge_catalog_path}'"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    logger.info(f"[QUALITY] Contratos validados com sucesso para fonte '{fct_table}' e catálogo IBGE.")


def build_dim_estabelecimento(
    conn: duckdb.DuckDBPyConnection,
    target_table: str = "dim_estabelecimento",
    fct_table: str = "fct_internacao",
    ibge_catalog_path: Optional[str] = None,
    cnes_catalog_path: Optional[str] = None,
    cadastral_table: Optional[str] = None,
) -> None:
    """
    Materializa a dimensão Gold dim_estabelecimento no DuckDB DW a partir
    estritamente dos dados factuais reais, do catálogo oficial do IBGE e do catálogo oficial CNES.
    """
    logger.info(f"[GOLD] Iniciando materialização da dimensão {target_table}...")

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

    # 1. Define caminhos padrão dos catálogos
    if not ibge_catalog_path:
        ibge_catalog_path = os.path.join(base_dir, "config", "dim_municipios_ibge.parquet")
    if not cnes_catalog_path:
        cnes_catalog_path = os.path.join(base_dir, "config", "dim_cnes_datasus.parquet")

    # 2. Validação estrita do contrato Silver e catálogo IBGE (Fail Fast)
    _validate_source_contracts(conn, fct_table=fct_table, ibge_catalog_path=ibge_catalog_path)
    ibge_path_sql = ibge_catalog_path.replace("\\", "/")

    # 3. Detecção e auditoria de inconsistências de CNES (múltiplos municípios ou UFs)
    sql_inconsistencias = f"""
    SELECT
        TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR)) AS cnes,
        COUNT(DISTINCT TRIM(CAST(codigo_municipio_hospital AS VARCHAR))) AS qtd_municipios,
        COUNT(DISTINCT TRIM(CAST(uf AS VARCHAR))) AS qtd_ufs
    FROM {fct_table}
    WHERE codigo_estabelecimento_cnes IS NOT NULL 
      AND TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR)) != ''
    GROUP BY TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR))
    HAVING COUNT(DISTINCT TRIM(CAST(codigo_municipio_hospital AS VARCHAR))) > 1
        OR COUNT(DISTINCT TRIM(CAST(uf AS VARCHAR))) > 1;
    """
    inconsistencias = conn.execute(sql_inconsistencias).fetchall()
    if inconsistencias:
        logger.warning(
            f"[DATA QUALITY AUDIT] Detectadas {len(inconsistencias)} inconsistências territoriais para CNES em '{fct_table}'."
        )
    else:
        logger.info(f"[DATA QUALITY AUDIT] Zero inconsistências territoriais de CNES detectadas em '{fct_table}'.")

    # 4. Configuração de Enriquecimento Cadastral (via Parquet ou Tabela Cadastral)
    existing_tables = {row[0].lower() for row in conn.execute("SHOW TABLES;").fetchall()}
    has_cadastral_table = cadastral_table and cadastral_table.lower() in existing_tables
    has_cnes_parquet = cnes_catalog_path and os.path.exists(cnes_catalog_path)

    if has_cadastral_table:
        logger.info(f"[GOLD] Enriquecendo com cadastro da tabela '{cadastral_table}'.")
        cad_cols = _get_table_columns(conn, cadastral_table)
        nome_expr = "cad.nome_fantasia" if "nome_fantasia" in cad_cols else "CAST(NULL AS VARCHAR)"
        razao_expr = "cad.razao_social" if "razao_social" in cad_cols else "CAST(NULL AS VARCHAR)"
        tipo_expr = "cad.tipo_unidade" if "tipo_unidade" in cad_cols else "CAST(NULL AS VARCHAR)"
        cadastral_join = f"""
        LEFT JOIN {cadastral_table} cad
            ON c.codigo_estabelecimento_cnes = LPAD(TRIM(CAST(cad.codigo_estabelecimento_cnes AS VARCHAR)), 7, '0')
        """
    elif has_cnes_parquet:
        cnes_path_sql = cnes_catalog_path.replace("\\", "/")
        logger.info(f"[GOLD] Enriquecendo com catálogo oficial CNES '{cnes_path_sql}'.")
        nome_expr = "cad.nome_fantasia"
        razao_expr = "cad.razao_social"
        tipo_expr = "cad.tipo_unidade"
        cadastral_join = f"""
        LEFT JOIN '{cnes_path_sql}' cad
            ON c.codigo_estabelecimento_cnes = LPAD(TRIM(CAST(cad.codigo_estabelecimento_cnes AS VARCHAR)), 7, '0')
        """
    else:
        nome_expr = "CAST(NULL AS VARCHAR)"
        razao_expr = "CAST(NULL AS VARCHAR)"
        tipo_expr = "CAST(NULL AS VARCHAR)"
        cadastral_join = ""

    # 5. SQL Canônico de Materialização Gold (Processamento nativo e idempotente em DuckDB)
    sql_build_gold = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH cnes_fatos AS (
        -- Agrupa estabelecimentos válidos presentes na Fato
        SELECT
            LPAD(TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR)), 7, '0') AS codigo_estabelecimento_cnes,
            ANY_VALUE(TRIM(CAST(codigo_municipio_hospital AS VARCHAR))) AS codigo_municipio_hospital,
            ANY_VALUE(TRIM(CAST(uf AS VARCHAR))) AS uf_fato
        FROM {fct_table}
        WHERE codigo_estabelecimento_cnes IS NOT NULL 
          AND TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR)) != ''
        GROUP BY LPAD(TRIM(CAST(codigo_estabelecimento_cnes AS VARCHAR)), 7, '0')
    ),
    cnes_enriquecido AS (
        SELECT
            c.codigo_estabelecimento_cnes,
            {nome_expr} AS nome_fantasia,
            {razao_expr} AS razao_social,
            COALESCE(m.municipality_code_6, LPAD(c.codigo_municipio_hospital, 6, '0')) AS codigo_municipio_ibge,
            m.municipality_name AS municipio,
            COALESCE(m.uf_sigla, c.uf_fato) AS uf,
            m.estado_nome,
            m.regiao,
            {tipo_expr} AS tipo_unidade,
            CURRENT_TIMESTAMP AS criado_em
        FROM cnes_fatos c
        LEFT JOIN '{ibge_path_sql}' m
            ON LPAD(c.codigo_municipio_hospital, 6, '0') = m.municipality_code_6
            OR LPAD(c.codigo_municipio_hospital, 7, '0') = m.municipality_code_7
        {cadastral_join}
    )
    SELECT * FROM cnes_enriquecido
    ORDER BY codigo_estabelecimento_cnes;
    """

    conn.execute(sql_build_gold)

    # 6. Auditoria de Qualidade e Métricas Pós-Materialização
    total_count = conn.execute(f"SELECT COUNT(*) FROM {target_table};").fetchone()[0]
    distinct_cnes = conn.execute(f"SELECT COUNT(DISTINCT codigo_estabelecimento_cnes) FROM {target_table};").fetchone()[0]
    resolvidos_ibge = conn.execute(f"SELECT COUNT(*) FROM {target_table} WHERE municipio IS NOT NULL;").fetchone()[0]
    nomes_preenchidos = conn.execute(f"SELECT COUNT(*) FROM {target_table} WHERE nome_fantasia IS NOT NULL;").fetchone()[0]

    logger.info(
        f"[GOLD] Dimensão {target_table} materializada com sucesso! "
        f"Total: {total_count} estabelecimentos | CNES únicos: {distinct_cnes} | "
        f"Nomes CNES oficiais: {nomes_preenchidos} ({nomes_preenchidos/total_count*100.0:.1f}%) | "
        f"Municípios IBGE: {resolvidos_ibge} ({resolvidos_ibge/total_count*100.0:.1f}%)."
    )

    # 7. Asserção estrita de integridade dimensional (Grain: 1 linha por CNES)
    assert total_count == distinct_cnes, (
        f"[FATAL INTEGRITY ERROR] Violação de unicidade de chave primária em {target_table}: "
        f"total={total_count} != distinct={distinct_cnes}"
    )
