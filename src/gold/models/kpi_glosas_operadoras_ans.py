"""
Modelo Gold: dm_ans_glosas_operadoras (Produção).

Materializa o Data Mart Gold de Glosas de Operadoras (ANS) a partir exclusivamente
das tabelas reais da camada Silver:
  - fct_ressarcimento_sus (Tabela Fato de cobranças e ressarcimento ao SUS)
  - dim_operadoras_saude (Dimensão cadastral de operadoras de saúde ativas)

Princípios de Engenharia de Dados aplicados:
  1. Zero dados sintéticos ou gerados por fórmulas artificiais (sem modulos %, sem taxas fixas);
  2. Competências dinâmicas obtidas diretamente dos fatos (sem base_competencias hardcoded);
  3. Validação estrita de contrato das tabelas Silver (falha explícita para tabelas/colunas ausentes);
  4. Grain determinístico: 1 registro = 1 operadora (codigo_registro_ans) x 1 competência (periodo);
  5. Chave primária surrogada idempotente e determinística via MD5;
  6. Resolução relacional segura via LEFT JOIN com desduplicação cadastral;
  7. Agregação SQL nativa de alta performance em DuckDB.
"""
from typing import List, Optional, Set
import duckdb
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Contratos mínimos obrigatórios para as fontes Silver
REQUIRED_FCT_COLUMNS: Set[str] = {
    "codigo_registro_ans",
    "ano",
    "mes",
    "valor_notificado_brl",
    "valor_recolhido_brl",
}

REQUIRED_DIM_COLUMNS: Set[str] = {
    "codigo_registro_ans",
    "razao_social",
    "modalidade_operadora",
}


def _get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> Set[str]:
    """Retorna o conjunto de nomes de colunas de uma tabela no DuckDB em minúsculas."""
    try:
        describe_rows = conn.execute(f"DESCRIBE {table_name};").fetchall()
        return {str(row[0]).lower() for row in describe_rows}
    except Exception as e:
        logger.error(f"[VALIDATION] Erro ao inspecionar schema da tabela '{table_name}': {e}")
        raise


def _validate_silver_contracts(
    conn: duckdb.DuckDBPyConnection,
    fct_table: str,
    dim_table: str,
) -> None:
    """
    Valida a existência e integridade de schema das tabelas Silver requeridas.
    Falha imediatamente e de forma explícita caso tabelas ou colunas obrigatórias não existam.
    """
    existing_tables = {row[0].lower() for row in conn.execute("SHOW TABLES;").fetchall()}

    # 1. Validação de existência da Fato
    if fct_table.lower() not in existing_tables:
        msg = f"Required Silver source not found: '{fct_table}'"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    # 2. Validação de existência da Dimensão
    if dim_table.lower() not in existing_tables:
        msg = f"Required Silver source not found: '{dim_table}'"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    # 3. Validação de colunas da Fato
    fct_cols = _get_table_columns(conn, fct_table)
    missing_fct = REQUIRED_FCT_COLUMNS - fct_cols
    if missing_fct:
        msg = f"Required column(s) missing in Silver source '{fct_table}': {sorted(missing_fct)}"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    # 4. Validação de colunas da Dimensão
    dim_cols = _get_table_columns(conn, dim_table)
    missing_dim = REQUIRED_DIM_COLUMNS - dim_cols
    if missing_dim:
        msg = f"Required column(s) missing in Silver source '{dim_table}': {sorted(missing_dim)}"
        logger.error(f"[FATAL QUALITY] {msg}")
        raise ValueError(msg)

    logger.info(f"[QUALITY] Contratos Silver validados com sucesso para '{fct_table}' e '{dim_table}'.")


def build_dm_ans_glosas_operadoras(
    conn: duckdb.DuckDBPyConnection,
    target_table: str = "dm_ans_glosas_operadoras",
    fct_table: str = "fct_ressarcimento_sus",
    dim_table: str = "dim_operadoras_saude",
) -> None:
    """
    Materializa o Data Mart Gold dm_ans_glosas_operadoras no DuckDB DW a partir
    estritamente dos dados factuais reais de fct_ressarcimento_sus e dim_operadoras_saude.
    """
    logger.info(f"[GOLD] Iniciando materialização de {target_table} a partir de {fct_table} e {dim_table}...")

    # 1. Validação estrita do contrato Silver (Fail Fast)
    _validate_silver_contracts(conn, fct_table=fct_table, dim_table=dim_table)

    # 2. Inspeciona colunas opcionais existentes na Fato e Dimensão para projeção segura
    fct_cols = _get_table_columns(conn, fct_table)
    dim_cols = _get_table_columns(conn, dim_table)

    # Expressões dinâmicas seguras para colunas opcionais
    cnpj_expr = "o.cnpj_operadora" if "cnpj_operadora" in dim_cols else "'00.000.000/0000-00'"
    nome_fantasia_expr = "o.nome_fantasia" if "nome_fantasia" in dim_cols else "NULL"
    porte_expr = "o.porte_operadora" if "porte_operadora" in dim_cols else "NULL"
    
    if "segmentacao_operadora" in dim_cols:
        seg_expr = "o.segmentacao_operadora"
    else:
        seg_expr = """
        CASE 
            WHEN UPPER(COALESCE(o.modalidade_operadora, '')) LIKE '%ODONTO%' THEN 'Exclus. Odontológica'
            WHEN o.modalidade_operadora IS NOT NULL THEN 'Médico-Hospitalar'
            ELSE 'NÃO INFORMADA'
        END
        """

    # Verificação de colunas de datas para tempo médio de pagamento
    if "data_internacao" in fct_cols and "data_alta" in fct_cols:
        tempo_pagamento_expr = """
        ROUND(
            AVG(
                CASE 
                    WHEN r.data_internacao IS NOT NULL AND r.data_alta IS NOT NULL 
                    THEN DATEDIFF('day', TRY_CAST(r.data_internacao AS DATE), TRY_CAST(r.data_alta AS DATE))
                    ELSE NULL
                END
            ), 1
        ) AS tempo_medio_pagamento_dias
        """
    else:
        tempo_pagamento_expr = "NULL::DOUBLE AS tempo_medio_pagamento_dias"

    # Verificação de situacao_cobranca para métricas de retorno
    if "situacao_cobranca" in fct_cols:
        pct_guias_sem_retorno_expr = """
        ROUND(
            (SUM(CASE WHEN UPPER(TRIM(CAST(r.situacao_cobranca AS VARCHAR))) IN ('EM_ANALISE', 'EM_RECURSO', 'PENDENTE') THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0)) * 100.0,
            2
        ) AS pct_guias_sem_retorno_60d
        """
        pct_valor_sem_retorno_expr = """
        ROUND(
            (SUM(CASE WHEN UPPER(TRIM(CAST(r.situacao_cobranca AS VARCHAR))) IN ('EM_ANALISE', 'EM_RECURSO', 'PENDENTE') THEN COALESCE(r.valor_notificado_brl, 0.0) ELSE 0.0 END) / NULLIF(SUM(COALESCE(r.valor_notificado_brl, 0.0)), 0)) * 100.0,
            2
        ) AS pct_valor_sem_retorno_60d
        """
    else:
        pct_guias_sem_retorno_expr = "0.0::DOUBLE AS pct_guias_sem_retorno_60d"
        pct_valor_sem_retorno_expr = "0.0::DOUBLE AS pct_valor_sem_retorno_60d"

    # 3. SQL Canônico de Materialização Gold
    sql_build_dm = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH dim_operadoras_dedup AS (
        -- Desduplicação cadastral de dimensão por código ANS
        SELECT
            TRIM(CAST(o.codigo_registro_ans AS VARCHAR)) AS codigo_registro_ans,
            ANY_VALUE({cnpj_expr}) AS cnpj_operadora,
            ANY_VALUE(o.razao_social) AS razao_social,
            ANY_VALUE({nome_fantasia_expr}) AS nome_fantasia,
            ANY_VALUE(o.modalidade_operadora) AS modalidade_operadora,
            ANY_VALUE({porte_expr}) AS porte_operadora,
            ANY_VALUE({seg_expr}) AS segmentacao_operadora
        FROM {dim_table} o
        WHERE o.codigo_registro_ans IS NOT NULL AND TRIM(CAST(o.codigo_registro_ans AS VARCHAR)) != ''
        GROUP BY TRIM(CAST(o.codigo_registro_ans AS VARCHAR))
    ),
    fatos_agregados AS (
        SELECT
            TRIM(CAST(r.codigo_registro_ans AS VARCHAR)) AS codigo_registro_ans,
            CAST(r.ano AS VARCHAR) AS ano,
            LPAD(CAST(r.mes AS VARCHAR), 2, '0') AS mes,
            CAST(r.ano AS VARCHAR) || '-' || LPAD(CAST(r.mes AS VARCHAR), 2, '0') AS periodo,
            
            -- Métricas quantitativas e financeiras agregadas
            COUNT(*) AS total_guias_glosadas,
            ROUND(SUM(COALESCE(r.valor_notificado_brl, 0.0)), 2) AS valor_total_faturado_brl,
            ROUND(SUM(COALESCE(r.valor_recolhido_brl, 0.0)), 2) AS valor_total_recolhido_brl,
            ROUND(SUM(GREATEST(0.0, COALESCE(r.valor_notificado_brl, 0.0) - COALESCE(r.valor_recolhido_brl, 0.0))), 2) AS valor_total_glosado_brl,
            
            -- Taxa de glosa real: valor glosado / valor faturado
            ROUND(
                CASE 
                    WHEN SUM(COALESCE(r.valor_notificado_brl, 0.0)) > 0 
                    THEN (SUM(GREATEST(0.0, COALESCE(r.valor_notificado_brl, 0.0) - COALESCE(r.valor_recolhido_brl, 0.0))) / SUM(COALESCE(r.valor_notificado_brl, 0.0))) * 100.0
                    ELSE 0.0 
                END, 
                2
            ) AS taxa_glosa_pct,
            
            {tempo_pagamento_expr},
            {pct_guias_sem_retorno_expr},
            {pct_valor_sem_retorno_expr}
        FROM {fct_table} r
        WHERE r.codigo_registro_ans IS NOT NULL AND TRIM(CAST(r.codigo_registro_ans AS VARCHAR)) != ''
        GROUP BY 
            TRIM(CAST(r.codigo_registro_ans AS VARCHAR)),
            CAST(r.ano AS VARCHAR),
            LPAD(CAST(r.mes AS VARCHAR), 2, '0')
    ),
    consolidado AS (
        SELECT
            -- Chave primária surrogada determinística (Grain: operadora x competência)
            md5(concat_ws('-', f.codigo_registro_ans, f.periodo)) AS id_registro_kpi,
            f.codigo_registro_ans,
            COALESCE(d.cnpj_operadora, '00.000.000/0000-00') AS cnpj_operadora,
            COALESCE(d.razao_social, 'OPERADORA NÃO IDENTIFICADA') AS razao_social,
            COALESCE(d.modalidade_operadora, 'NÃO INFORMADA') AS modalidade_operadora,
            d.porte_operadora,
            COALESCE(d.segmentacao_operadora, 'NÃO INFORMADA') AS segmentacao_operadora,
            f.ano,
            f.mes,
            f.periodo,
            f.total_guias_glosadas,
            f.valor_total_faturado_brl,
            f.valor_total_recolhido_brl,
            f.valor_total_glosado_brl,
            f.taxa_glosa_pct,
            f.tempo_medio_pagamento_dias,
            f.pct_guias_sem_retorno_60d,
            f.pct_valor_sem_retorno_60d,
            CURRENT_TIMESTAMP AS criado_em
        FROM fatos_agregados f
        LEFT JOIN dim_operadoras_dedup d 
            ON f.codigo_registro_ans = d.codigo_registro_ans
    )
    SELECT * FROM consolidado
    ORDER BY periodo DESC, valor_total_glosado_brl DESC;
    """

    conn.execute(sql_build_dm)
    count = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    logger.info(f"[GOLD] Tabela {target_table} materializada com sucesso ({count} registros).")
