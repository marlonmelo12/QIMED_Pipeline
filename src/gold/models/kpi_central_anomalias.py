"""
Central de Anomalias e Auditoria Hospitalar - QIMED Lakehouse V3 (Gold Layer).
Motor anal?tico para identificação de outliers estatísticos e inconformidades clínicas
com c?lculo do excesso de custo sobre o percentil 90 e gestão de workflow audit?vel.
"""
import duckdb
import pandas as pd
from typing import Optional, Dict, Any

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def get_central_anomalias_sql(fct_internacao_source: str = "fct_internacao", min_proc_count: int = 5) -> str:
    """
    Retorna a query SQL DuckDB que computa percentis (P90, P99) e detecta as 3 regras canônicas de anomalia.
    """
    return f"""
    WITH stats_procedimentos AS (
        SELECT 
            codigo_procedimento_realizado,
            COUNT(*) AS total_procedimentos,
            QUANTILE_CONT(valor_total_brl, 0.90) AS p90_custo,
            QUANTILE_CONT(valor_total_brl, 0.99) AS p99_custo
        FROM {fct_internacao_source}
        WHERE valor_total_brl > 0
        GROUP BY codigo_procedimento_realizado
        HAVING COUNT(*) >= {min_proc_count}
    )
    -- Regra 1: Outliers de Custo Extremo (> P99)
    SELECT
        md5(concat_ws('-', CAST(f.numero_aih AS VARCHAR), 'ANOMALIA_CUSTO_P99')) AS id_alerta,
        f.numero_aih,
        f.codigo_estabelecimento_cnes,
        f.uf,
        '2026-05' AS periodo,
        f.codigo_procedimento_realizado,
        'OUTLIER_CUSTO_P99' AS tipo_anomalia,
        'ALTA' AS severidade,
        f.valor_total_brl AS valor_faturado_brl,
        s.p90_custo AS custo_esperado_brl,
        GREATEST(0.0, f.valor_total_brl - s.p90_custo) AS excesso_custo_brl,
        'NOVA' AS status_operacional,
        CURRENT_TIMESTAMP AS data_geracao,
        CURRENT_TIMESTAMP AS criado_em,
        'v1.0' AS versao_regra
    FROM {fct_internacao_source} f
    JOIN stats_procedimentos s ON f.codigo_procedimento_realizado = s.codigo_procedimento_realizado
    WHERE f.valor_total_brl > s.p99_custo

    UNION ALL

    -- Regra 2: AIHs Iniciais com Valor Total Zero
    SELECT
        md5(concat_ws('-', CAST(f.numero_aih AS VARCHAR), 'VALOR_ZERO')) AS id_alerta,
        f.numero_aih,
        f.codigo_estabelecimento_cnes,
        f.uf,
        '2026-05' AS periodo,
        f.codigo_procedimento_realizado,
        'AIH_VALOR_ZERO' AS tipo_anomalia,
        'MEDIA' AS severidade,
        0.0 AS valor_faturado_brl,
        0.0 AS custo_esperado_brl,
        0.0 AS excesso_custo_brl,
        'NOVA' AS status_operacional,
        CURRENT_TIMESTAMP AS data_geracao,
        CURRENT_TIMESTAMP AS criado_em,
        'v1.0' AS versao_regra
    FROM {fct_internacao_source} f
    WHERE f.tipo_identificacao_aih = '1' AND (f.valor_total_brl = 0.0 OR f.valor_total_brl IS NULL)

    UNION ALL

    -- Regra 3: Óbitos Imediatos / Permanência Zero
    SELECT
        md5(concat_ws('-', CAST(f.numero_aih AS VARCHAR), 'OBITO_IMEDIATO')) AS id_alerta,
        f.numero_aih,
        f.codigo_estabelecimento_cnes,
        f.uf,
        '2026-05' AS periodo,
        f.codigo_procedimento_realizado,
        'OBITO_PERMANENCIA_ZERO' AS tipo_anomalia,
        'CRITICA' AS severidade,
        f.valor_total_brl AS valor_faturado_brl,
        0.0 AS custo_esperado_brl,
        0.0 AS excesso_custo_brl,
        'NOVA' AS status_operacional,
        CURRENT_TIMESTAMP AS data_geracao,
        CURRENT_TIMESTAMP AS criado_em,
        'v1.0' AS versao_regra
    FROM {fct_internacao_source} f
    WHERE f.indicador_obito = TRUE AND f.dias_permanencia_real = 0
    """


def build_aud_alertas_anomalias(
    conn: duckdb.DuckDBPyConnection,
    fct_internacao_source: str = "fct_internacao",
    target_table: str = "aud_alertas_anomalias"
) -> int:
    """
    Executa e materializa a tabela física de auditoria aud_alertas_anomalias no DuckDB.
    Retorna o número de alertas gerados.
    """
    query = get_central_anomalias_sql(fct_internacao_source=fct_internacao_source)
    sql_create = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    {query}
    """
    conn.execute(sql_create)
    count = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    logger.info(f"Tabela de auditoria {target_table} materializada com {count} alertas de anomalia.")
    return count
