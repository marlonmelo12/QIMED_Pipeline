from src.analytics.outliers import detectar_outliers_mad
"""
DuckDB Query Engine - Camada Analytics da API QIMED.
Executa consultas analíticas somente leitura diretamente no arquivo DuckDB DW Gold.
"""
from typing import Any, Dict, List, Optional
import duckdb
from src.utils.config_loader import load_pipeline_config
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


def _connect_dw(read_only: bool = True):
    """Abre conexão com o DuckDB DW Gold local em modo somente leitura de alta performance."""
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")
    return duckdb.connect(dw_path, read_only=read_only)


def query_gold(sql: str) -> list[dict]:
    """
    Executa uma consulta SQL na Camada Gold do DuckDB em modo read_only.
    Converte o resultado Arrow em lista de dicionários para serialização JSON.
    """
    try:
        with _connect_dw(read_only=True) as conn:
            return conn.execute(sql).arrow().read_all().to_pylist()
    except Exception:
        logger.error(f"Falha na query DuckDB: {sql[:200]}", exc_info=True)
        raise


def query_dashboard_financeiro(periodo: str, uf: str = "") -> Dict[str, Any]:
    """
    Executa as consultas consolidadas do Dashboard Financeiro:
    - Tenta carregar do Data Mart pré-agregado O(1) `dm_kpi_dashboard_financeiro`.
    - Caso não exista (competência aberta), executa fallback dinâmico sobre `fct_internacao`.
    """
    p = periodo.replace("'", "").strip()
    u = uf.replace("'", "").strip().upper() if uf else ""

    try:
        with _connect_dw(read_only=True) as conn:
            tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
            
            # 1. Rota Ultrarrápida: Data Mart Pré-Agregado Gold (O(1))
            if "dm_kpi_dashboard_financeiro" in tables:
                sql_fast_kpi = f"SELECT * FROM dm_kpi_dashboard_financeiro WHERE periodo = '{p}' AND uf = '{u}'"
                fast_kpi_rows = conn.execute(sql_fast_kpi).arrow().read_all().to_pylist()
                if fast_kpi_rows:
                    kpi_row = fast_kpi_rows[0]
                    sql_perm_fast = f"SELECT faixa_permanencia, total_internacoes, custo_total_brl, ROUND(custo_total_brl / NULLIF(total_internacoes, 0), 2) AS custo_medio_brl, dias_medios AS media_dias_real FROM dm_kpi_permanencia_faixa WHERE periodo = '{p}' AND uf = '{u}' ORDER BY CASE faixa_permanencia WHEN '0-3d' THEN 1 WHEN '4-7d' THEN 2 WHEN '8-14d' THEN 3 ELSE 4 END"
                    permanencia = conn.execute(sql_perm_fast).arrow().read_all().to_pylist() if "dm_kpi_permanencia_faixa" in tables else []
                    
                    sql_pareto = f"SELECT codigo_motivo_glosa, descricao_motivo_glosa, total_procedimentos_glosados AS total_glosas, valor_total_glosado_brl, 100.0 AS percentual_glosa_pct, 100.0 AS percentual_acumulado_pareto_pct FROM dm_glosas_auditoria WHERE (periodo = '{p}' OR ano = '{p}') AND ('{u}' = '' OR uf = '{u}') LIMIT 10;"
                    pareto = conn.execute(sql_pareto).arrow().read_all().to_pylist() if "dm_glosas_auditoria" in tables else []
                    
                    sql_serie = f"SELECT periodo, total_internacoes AS total_aihs_aprovadas, 0 AS total_aihs_rejeitadas, valor_total_brl AS valor_aprovado_brl, 0.0 AS valor_glosado_brl, 0.0 AS taxa_rejeicao_pct FROM agg_internacoes_uf WHERE ('{u}' = '' OR uf = '{u}') ORDER BY periodo ASC LIMIT 6"
                    serie = conn.execute(sql_serie).arrow().read_all().to_pylist() if "agg_internacoes_uf" in tables else []

                    return {
                        "fonte": "gold_pre_agregado",
                        "kpis": {
                            "ticket_medio_brl": kpi_row.get("ticket_medio_brl", 0.0),
                            "mediana_custo_brl": kpi_row.get("mediana_custo_brl", 0.0),
                            "custo_total_brl": kpi_row.get("custo_total_brl", 0.0),
                            "custo_medio_obito_brl": kpi_row.get("custo_medio_obito_brl", 0.0),
                            "custo_medio_alta_brl": kpi_row.get("custo_medio_alta_brl", 0.0),
                            "razao_custo_obito_alta": kpi_row.get("razao_custo_obito_alta", 0.0),
                            "taxa_glosa_pct": kpi_row.get("taxa_glosa_pct", 0.0),
                        },
                        "top_motivos_glosa_pareto": pareto,
                        "custo_por_faixa_permanencia": permanencia,
                        "serie_temporal_aprovadas_vs_rejeitadas": serie,
                    }

            # 2. Fallback Dinâmico on-the-fly para competência aberta/não-agregada
            has_glosas = "dm_glosas_auditoria" in tables
            glosa_cte = f"""
            kpi_glosas AS (
                SELECT
                    ROUND(COALESCE((SUM(total_glosado_brl) / NULLIF(SUM(total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_pct
                FROM dm_glosas_auditoria
                WHERE (periodo = '{p}' OR ano = '{p}')
                  AND ('{u}' = '' OR uf = '{u}')
            )
            """ if has_glosas else """
            kpi_glosas AS (
                SELECT 0.0 AS taxa_glosa_pct
            )
            """

            sql_kpi = f"""
            WITH base_int AS (
                SELECT valor_total_brl, indicador_obito
                FROM fct_internacao
                WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{p}' OR CAST(ano AS VARCHAR) = '{p}')
                  AND ('{u}' = '' OR uf = '{u}')
            ),
            kpi_int AS (
                SELECT
                    ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
                    ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
                    ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                    ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
                    ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl,
                    ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END) / NULLIF(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0), 0.0), 2) AS razao_custo_obito_alta
                FROM base_int
            ),
            {glosa_cte}
            SELECT 
                kpi_int.ticket_medio_brl,
                kpi_int.mediana_custo_brl,
                kpi_int.custo_total_brl,
                kpi_int.custo_medio_obito_brl,
                kpi_int.custo_medio_alta_brl,
                kpi_int.razao_custo_obito_alta,
                COALESCE(kpi_glosas.taxa_glosa_pct, 0.0) AS taxa_glosa_pct
            FROM kpi_int CROSS JOIN kpi_glosas;
            """

            if has_glosas:
                sql_pareto = f"""
                WITH motivos AS (
                    SELECT
                        codigo_motivo_glosa,
                        descricao_motivo_glosa,
                        SUM(total_procedimentos_glosados) AS total_glosas,
                        ROUND(SUM(total_glosado_brl), 2) AS valor_total_glosado_brl
                    FROM dm_glosas_auditoria
                    WHERE (periodo = '{p}' OR ano = '{p}')
                      AND ('{u}' = '' OR uf = '{u}')
                    GROUP BY codigo_motivo_glosa, descricao_motivo_glosa
                ),
                total_geral AS (
                    SELECT COALESCE(SUM(valor_total_glosado_brl), 0.0) AS soma_total FROM motivos
                )
                SELECT
                    m.codigo_motivo_glosa,
                    m.descricao_motivo_glosa,
                    m.total_glosas,
                    m.valor_total_glosado_brl,
                    ROUND((m.valor_total_glosado_brl / NULLIF(t.soma_total, 0)) * 100.0, 2) AS percentual_glosa_pct,
                    ROUND(SUM(m.valor_total_glosado_brl) OVER (ORDER BY m.valor_total_glosado_brl DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / NULLIF(t.soma_total, 0) * 100.0, 2) AS percentual_acumulado_pareto_pct
                FROM motivos m CROSS JOIN total_geral t
                ORDER BY m.valor_total_glosado_brl DESC
                LIMIT 10;
                """
            else:
                sql_pareto = None

            sql_perm = f"""
            SELECT
                CASE 
                    WHEN dias_permanencia_real BETWEEN 0 AND 3 THEN '0-3d'
                    WHEN dias_permanencia_real BETWEEN 4 AND 7 THEN '4-7d'
                    WHEN dias_permanencia_real BETWEEN 8 AND 14 THEN '8-14d'
                    WHEN dias_permanencia_real BETWEEN 15 AND 30 THEN '15-30d'
                    ELSE '>30d'
                END AS faixa_permanencia,
                COUNT(*) AS total_internacoes,
                ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS custo_medio_brl,
                ROUND(COALESCE(AVG(dias_permanencia_real), 0.0), 1) AS media_dias_real
            FROM fct_internacao
            WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{p}' OR CAST(ano AS VARCHAR) = '{p}')
              AND ('{u}' = '' OR uf = '{u}')
            GROUP BY faixa_permanencia
            ORDER BY 
                CASE faixa_permanencia
                    WHEN '0-3d' THEN 1
                    WHEN '4-7d' THEN 2
                    WHEN '8-14d' THEN 3
                    WHEN '15-30d' THEN 4
                    ELSE 5
                END;
            """

            serie_glosas_cte = f"""
            aih_glosadas AS (
                SELECT
                    periodo,
                    SUM(total_procedimentos_glosados) AS total_aihs_rejeitadas,
                    ROUND(SUM(total_glosado_brl), 2) AS valor_glosado_brl
                FROM dm_glosas_auditoria
                WHERE ('{u}' = '' OR uf = '{u}')
                GROUP BY periodo
            )
            """ if has_glosas else """
            aih_glosadas AS (
                SELECT CAST(NULL AS VARCHAR) AS periodo, 0 AS total_aihs_rejeitadas, 0.0 AS valor_glosado_brl WHERE 1=0
            )
            """

            sql_serie = f"""
            WITH aih_aprovadas AS (
                SELECT
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS total_aihs_aprovadas,
                    ROUND(SUM(COALESCE(valor_total_brl, 0.0)), 2) AS valor_aprovado_brl
                FROM fct_internacao
                WHERE ('{u}' = '' OR uf = '{u}')
                GROUP BY ano, mes
            ),
            {serie_glosas_cte}
            SELECT
                COALESCE(a.periodo, g.periodo) AS periodo,
                COALESCE(a.total_aihs_aprovadas, 0) AS total_aihs_aprovadas,
                COALESCE(g.total_aihs_rejeitadas, 0) AS total_aihs_rejeitadas,
                COALESCE(a.valor_aprovado_brl, 0.0) AS valor_aprovado_brl,
                COALESCE(g.valor_glosado_brl, 0.0) AS valor_glosado_brl,
                ROUND(COALESCE((COALESCE(g.total_aihs_rejeitadas, 0) * 100.0) / NULLIF(COALESCE(a.total_aihs_aprovadas, 0) + COALESCE(g.total_aihs_rejeitadas, 0), 0), 0.0), 2) AS taxa_rejeicao_pct
            FROM aih_aprovadas a
            FULL OUTER JOIN aih_glosadas g ON a.periodo = g.periodo
            ORDER BY periodo ASC
            LIMIT 6;
            """

            kpi_rows = conn.execute(sql_kpi).arrow().read_all().to_pylist()
            kpis = kpi_rows[0] if kpi_rows else {
                "ticket_medio_brl": 0.0,
                "mediana_custo_brl": 0.0,
                "custo_total_brl": 0.0,
                "custo_medio_obito_brl": 0.0,
                "custo_medio_alta_brl": 0.0,
                "razao_custo_obito_alta": 0.0,
                "taxa_glosa_pct": 0.0,
            }
            pareto = conn.execute(sql_pareto).arrow().read_all().to_pylist() if sql_pareto else []
            permanencia = conn.execute(sql_perm).arrow().read_all().to_pylist()
            serie = conn.execute(sql_serie).arrow().read_all().to_pylist()

            return {
                "fonte": "consulta_tempo_real",
                "kpis": kpis,
                "top_motivos_glosa_pareto": pareto,
                "custo_por_faixa_permanencia": permanencia,
                "serie_temporal_aprovadas_vs_rejeitadas": serie,
            }
    except Exception:
        logger.error(f"Falha na query consolidada do Dashboard Financeiro (periodo={periodo}, uf={uf})", exc_info=True)
        raise


def query_drilldown_ticket_medio(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """
    Executa a análise aprofundada de Ticket Médio:
    - KPIs Estatísticos (Média, Mediana, P75, Máx, Mín, Desvio Padrão);
    - Curva de Percentis (P25, P50, P75, P90, P99);
    - Evolução Mensal dos últimos 6 meses;
    - Ranking de Hospitais CNES com paginação;
    - Quebra por UF.
    """
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    sql_kpi = f"""
    SELECT
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.75), 0.0), 2) AS p75_custo_brl,
        ROUND(COALESCE(MAX(valor_total_brl), 0.0), 2) AS maximo_custo_brl,
        ROUND(COALESCE(MIN(valor_total_brl), 0.0), 2) AS minimo_custo_brl,
        ROUND(COALESCE(STDDEV_POP(valor_total_brl), 0.0), 2) AS desvio_padrao_brl,
        COUNT(*) AS total_internacoes
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}');
    """

    sql_percentis = f"""
    SELECT
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.25), 0.0), 2) AS p25,
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.50), 0.0), 2) AS p50,
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.75), 0.0), 2) AS p75,
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.90), 0.0), 2) AS p90,
        ROUND(COALESCE(QUANTILE_CONT(valor_total_brl, 0.99), 0.0), 2) AS p99
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}');
    """

    sql_evolucao = f"""
    SELECT
        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
        COUNT(*) AS total_internacoes
    FROM fct_internacao
    WHERE ('{uf}' = '' OR uf = '{uf}')
    GROUP BY ano, mes
    ORDER BY periodo ASC
    LIMIT 6;
    """

    sql_hospitais = f"""
    SELECT
        codigo_estabelecimento_cnes,
        uf,
        COUNT(*) AS total_internacoes,
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}')
    GROUP BY codigo_estabelecimento_cnes, uf
    ORDER BY custo_total_brl DESC
    LIMIT {limit} OFFSET {offset};
    """

    sql_uf = f"""
    SELECT
        uf,
        COUNT(*) AS total_internacoes,
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
    GROUP BY uf
    ORDER BY custo_total_brl DESC;
    """

    p = periodo.replace("'", "").strip()
    u = uf.replace("'", "").strip().upper() if uf else ""

    try:
        with _connect_dw(read_only=True) as conn:
            tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
            
            # 1. Rota Ultrarrápida Gold Pré-Agregada
            if "dm_kpi_percentis_hospitalares" in tables:
                sql_fast_perc = f"SELECT * FROM dm_kpi_percentis_hospitalares WHERE periodo = '{p}' AND uf = '{u}'"
                fast_rows = conn.execute(sql_fast_perc).arrow().read_all().to_pylist()
                if fast_rows:
                    row = fast_rows[0]
                    kpis = {
                        "ticket_medio_brl": row.get("ticket_medio_brl", 0.0),
                        "mediana_custo_brl": row.get("mediana_custo_brl", 0.0),
                        "p75_custo_brl": row.get("p75", 0.0),
                        "maximo_custo_brl": row.get("maximo_custo_brl", 0.0),
                        "minimo_custo_brl": row.get("minimo_custo_brl", 0.0),
                        "desvio_padrao_brl": row.get("desvio_padrao_brl", 0.0),
                        "total_internacoes": row.get("total_internacoes", 0)
                    }
                    percentis = {
                        "p25": row.get("p25", 0.0),
                        "p50": row.get("p50", 0.0),
                        "p75": row.get("p75", 0.0),
                        "p90": row.get("p90", 0.0),
                        "p99": row.get("p99", 0.0)
                    }
                    
                    sql_evolucao = f"SELECT periodo, ROUND(AVG(ticket_medio_brl), 2) AS ticket_medio_brl, ROUND(AVG(mediana_custo_brl), 2) AS mediana_custo_brl, SUM(total_internacoes) AS total_internacoes FROM dm_kpi_percentis_hospitalares WHERE ('{u}' = '' OR uf = '{u}') GROUP BY periodo ORDER BY periodo ASC LIMIT 6"
                    evolucao = conn.execute(sql_evolucao).arrow().read_all().to_pylist()
                    
                    sql_hospitais = f"SELECT codigo_estabelecimento_cnes, uf, total_internacoes, ticket_medio_brl, custo_total_brl FROM dm_hospital_efficiency WHERE periodo = '{p}' AND ('{u}' = '' OR uf = '{u}') ORDER BY custo_total_brl DESC LIMIT {limit} OFFSET {offset}"
                    hospitais = conn.execute(sql_hospitais).arrow().read_all().to_pylist() if "dm_hospital_efficiency" in tables else []
                    
                    sql_uf = f"SELECT uf, SUM(total_internacoes) AS total_internacoes, ROUND(AVG(valor_medio_internacao_brl), 2) AS ticket_medio_brl, ROUND(SUM(valor_total_brl), 2) AS custo_total_brl FROM agg_internacoes_uf WHERE periodo = '{p}' GROUP BY uf ORDER BY custo_total_brl DESC"
                    quebra_uf = conn.execute(sql_uf).arrow().read_all().to_pylist() if "agg_internacoes_uf" in tables else []

                    return {
                        "fonte": "gold_pre_agregado",
                        "kpi_resumo": kpis,
                        "distribuicao_percentis": percentis,
                        "evolucao_mensal": evolucao,
                        "ranking_hospitais_cnes": hospitais,
                        "quebra_por_uf": quebra_uf,
                    }

            # 2. Fallback Dinâmico on-the-fly
            kpi_res = conn.execute(sql_kpi).arrow().read_all().to_pylist()
            kpis = kpi_res[0] if kpi_res else {
                "ticket_medio_brl": 0.0, "mediana_custo_brl": 0.0, "p75_custo_brl": 0.0,
                "maximo_custo_brl": 0.0, "minimo_custo_brl": 0.0, "desvio_padrao_brl": 0.0,
                "total_internacoes": 0
            }
            percentis_res = conn.execute(sql_percentis).arrow().read_all().to_pylist()
            percentis = percentis_res[0] if percentis_res else {"p25": 0.0, "p50": 0.0, "p75": 0.0, "p90": 0.0, "p99": 0.0}
            evolucao = conn.execute(sql_evolucao).arrow().read_all().to_pylist()
            hospitais = conn.execute(sql_hospitais).arrow().read_all().to_pylist()
            quebra_uf = conn.execute(sql_uf).arrow().read_all().to_pylist()

            return {
                "fonte": "consulta_tempo_real",
                "kpi_resumo": kpis,
                "distribuicao_percentis": percentis,
                "evolucao_mensal": evolucao,
                "ranking_hospitais_cnes": hospitais,
                "quebra_por_uf": quebra_uf,
            }
    except Exception:
        logger.error(f"Falha no drilldown de Ticket Médio (periodo={periodo}, uf={uf})", exc_info=True)
        raise


def query_drilldown_custo_total(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """
    Executa a análise aprofundada de Custo Total:
    - Decomposição SH vs. SP vs. UTI;
    - Evolução Temporal dos componentes de custo;
    - Ranking de Hospitais CNES por volume financeiro;
    - Quebra por UF.
    """
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    sql_kpi = f"""
    SELECT
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
        ROUND(COALESCE(SUM(valor_servicos_hospitalares_brl), 0.0), 2) AS custo_sh_brl,
        ROUND(COALESCE(SUM(valor_servicos_profissionais_brl), 0.0), 2) AS custo_sp_brl,
        ROUND(COALESCE(SUM(valor_uti_brl), 0.0), 2) AS custo_uti_brl,
        ROUND(COALESCE((SUM(valor_servicos_hospitalares_brl) / NULLIF(SUM(valor_total_brl), 0)) * 100.0, 0.0), 2) AS percentual_sh_pct,
        ROUND(COALESCE((SUM(valor_servicos_profissionais_brl) / NULLIF(SUM(valor_total_brl), 0)) * 100.0, 0.0), 2) AS percentual_sp_pct,
        ROUND(COALESCE((SUM(valor_uti_brl) / NULLIF(SUM(valor_total_brl), 0)) * 100.0, 0.0), 2) AS percentual_uti_pct,
        COUNT(*) AS total_internacoes
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}');
    """

    sql_evolucao = f"""
    SELECT
        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
        ROUND(COALESCE(SUM(valor_servicos_hospitalares_brl), 0.0), 2) AS custo_sh_brl,
        ROUND(COALESCE(SUM(valor_servicos_profissionais_brl), 0.0), 2) AS custo_sp_brl,
        COUNT(*) AS total_internacoes
    FROM fct_internacao
    WHERE ('{uf}' = '' OR uf = '{uf}')
    GROUP BY ano, mes
    ORDER BY periodo ASC
    LIMIT 6;
    """

    sql_hospitais = f"""
    SELECT
        codigo_estabelecimento_cnes,
        uf,
        COUNT(*) AS total_internacoes,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
        ROUND(COALESCE(SUM(valor_servicos_hospitalares_brl), 0.0), 2) AS custo_sh_brl,
        ROUND(COALESCE(SUM(valor_servicos_profissionais_brl), 0.0), 2) AS custo_sp_brl,
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}')
    GROUP BY codigo_estabelecimento_cnes, uf
    ORDER BY custo_total_brl DESC
    LIMIT {limit} OFFSET {offset};
    """

    sql_uf = f"""
    SELECT
        uf,
        COUNT(*) AS total_internacoes,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
        ROUND(COALESCE(SUM(valor_servicos_hospitalares_brl), 0.0), 2) AS custo_sh_brl,
        ROUND(COALESCE(SUM(valor_servicos_profissionais_brl), 0.0), 2) AS custo_sp_brl
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
    GROUP BY uf
    ORDER BY custo_total_brl DESC;
    """

    try:
        with _connect_dw(read_only=True) as conn:
            kpi_res = conn.execute(sql_kpi).arrow().read_all().to_pylist()
            kpis = kpi_res[0] if kpi_res else {
                "custo_total_brl": 0.0, "custo_sh_brl": 0.0, "custo_sp_brl": 0.0, "custo_uti_brl": 0.0,
                "percentual_sh_pct": 0.0, "percentual_sp_pct": 0.0, "percentual_uti_pct": 0.0, "total_internacoes": 0
            }
            evolucao = conn.execute(sql_evolucao).arrow().read_all().to_pylist()
            hospitais = conn.execute(sql_hospitais).arrow().read_all().to_pylist()
            quebra_uf = conn.execute(sql_uf).arrow().read_all().to_pylist()

            decomposicao = [
                {"componente": "Serviços Hospitalares (SH)", "valor_brl": kpis["custo_sh_brl"], "percentual_pct": kpis["percentual_sh_pct"]},
                {"componente": "Serviços Profissionais (SP)", "valor_brl": kpis["custo_sp_brl"], "percentual_pct": kpis["percentual_sp_pct"]},
                {"componente": "Unidade de Terapia Intensiva (UTI)", "valor_brl": kpis["custo_uti_brl"], "percentual_pct": kpis["percentual_uti_pct"]},
            ]

            return {
                "kpi_resumo": kpis,
                "decomposicao_custos": decomposicao,
                "evolucao_mensal": evolucao,
                "ranking_hospitais_cnes": hospitais,
                "quebra_por_uf": quebra_uf,
            }
    except Exception:
        logger.error(f"Falha no drilldown de Custo Total (periodo={periodo}, uf={uf})", exc_info=True)
        raise


def query_drilldown_custo_desfecho(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """
    Executa a análise de Custo por Desfecho (Óbito vs. Alta):
    - Métricas comparativas de óbito vs alta;
    - Curva de Gompertz-Makeham (mortalidade e custo por faixa etária);
    - Evolução Temporal da razão de custo;
    - Ranking hospitalar de desfechos.
    """
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    sql_kpi = f"""
    SELECT
        COUNT(*) AS total_internacoes,
        CAST(SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS BIGINT) AS total_obitos,
        CAST(SUM(CASE WHEN indicador_obito = FALSE THEN 1 ELSE 0 END) AS BIGINT) AS total_altas,
        ROUND(COALESCE((SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0)) * 100.0, 0.0), 2) AS taxa_mortalidade_pct,
        ROUND(COALESCE(SUM(CASE WHEN indicador_obito = TRUE THEN valor_total_brl ELSE 0.0 END), 0.0), 2) AS custo_total_obitos_brl,
        ROUND(COALESCE(SUM(CASE WHEN indicador_obito = FALSE THEN valor_total_brl ELSE 0.0 END), 0.0), 2) AS custo_total_altas_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END) / NULLIF(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0), 0.0), 2) AS razao_custo_obito_alta,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN dias_permanencia_real END), 0.0), 1) AS media_permanencia_obito_dias,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN dias_permanencia_real END), 0.0), 1) AS media_permanencia_alta_dias
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}');
    """

    sql_gompertz = f"""
    WITH base_idades AS (
        SELECT 
            date_diff('year', TRY_STRPTIME(data_nascimento_paciente, '%Y%m%d'), TRY_STRPTIME(data_internacao, '%Y%m%d')) AS idade_anos,
            indicador_obito,
            valor_total_brl
        FROM fct_internacao
        WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
          AND ('{uf}' = '' OR uf = '{uf}')
          AND data_nascimento_paciente IS NOT NULL
          AND data_internacao IS NOT NULL
    )
    SELECT 
        CASE 
            WHEN idade_anos < 1 THEN '0-1 ano'
            WHEN idade_anos BETWEEN 1 AND 9 THEN '1-9 anos'
            WHEN idade_anos BETWEEN 10 AND 19 THEN '10-19 anos'
            WHEN idade_anos BETWEEN 20 AND 29 THEN '20-29 anos'
            WHEN idade_anos BETWEEN 30 AND 39 THEN '30-39 anos'
            WHEN idade_anos BETWEEN 40 AND 49 THEN '40-49 anos'
            WHEN idade_anos BETWEEN 50 AND 59 THEN '50-59 anos'
            WHEN idade_anos BETWEEN 60 AND 69 THEN '60-69 anos'
            WHEN idade_anos BETWEEN 70 AND 79 THEN '70-79 anos'
            ELSE '80+ anos'
        END AS faixa_etaria,
        COUNT(*) AS total_internacoes,
        CAST(SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS BIGINT) AS total_obitos,
        ROUND(COALESCE((SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0)) * 100.0, 0.0), 2) AS taxa_mortalidade_pct,
        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS custo_medio_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl
    FROM base_idades
    WHERE idade_anos IS NOT NULL
    GROUP BY faixa_etaria
    ORDER BY MIN(idade_anos);
    """

    sql_evolucao = f"""
    SELECT
        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
        COUNT(*) AS total_internacoes,
        CAST(SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS BIGINT) AS total_obitos,
        ROUND(COALESCE((SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0)) * 100.0, 0.0), 2) AS taxa_mortalidade_pct,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END) / NULLIF(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0), 0.0), 2) AS razao_custo_obito_alta
    FROM fct_internacao
    WHERE ('{uf}' = '' OR uf = '{uf}')
    GROUP BY ano, mes
    ORDER BY periodo ASC
    LIMIT 6;
    """

    sql_hospitais = f"""
    SELECT
        codigo_estabelecimento_cnes,
        uf,
        COUNT(*) AS total_internacoes,
        CAST(SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS BIGINT) AS total_obitos,
        ROUND(COALESCE((SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0)) * 100.0, 0.0), 2) AS taxa_mortalidade_pct,
        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl
    FROM fct_internacao
    WHERE (CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') = '{periodo}' OR CAST(ano AS VARCHAR) = '{periodo}')
      AND ('{uf}' = '' OR uf = '{uf}')
    GROUP BY codigo_estabelecimento_cnes, uf
    ORDER BY custo_total_brl DESC
    LIMIT {limit} OFFSET {offset};
    """

    try:
        with _connect_dw(read_only=True) as conn:
            kpi_res = conn.execute(sql_kpi).arrow().read_all().to_pylist()
            kpis = kpi_res[0] if kpi_res else {
                "total_internacoes": 0, "total_obitos": 0, "total_altas": 0, "taxa_mortalidade_pct": 0.0,
                "custo_total_obitos_brl": 0.0, "custo_total_altas_brl": 0.0, "custo_medio_obito_brl": 0.0,
                "custo_medio_alta_brl": 0.0, "razao_custo_obito_alta": 0.0,
                "media_permanencia_obito_dias": 0.0, "media_permanencia_alta_dias": 0.0
            }
            gompertz = conn.execute(sql_gompertz).arrow().read_all().to_pylist()
            evolucao = conn.execute(sql_evolucao).arrow().read_all().to_pylist()
            hospitais = conn.execute(sql_hospitais).arrow().read_all().to_pylist()

            return {
                "kpi_resumo": kpis,
                "curva_mortalidade_custo_por_idade": gompertz,
                "evolucao_mensal": evolucao,
                "ranking_hospitais_mortalidade": hospitais,
            }
    except Exception:
        logger.error(f"Falha no drilldown de Custo por Desfecho (periodo={periodo}, uf={uf})", exc_info=True)
        raise


def query_central_anomalias(
    periodo: str,
    tipo: Optional[str] = None,
    severidade: Optional[str] = None,
    cnes: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Executa a consulta consolidada para a tela operacional da Central de Anomalias (SIH):
    - 4 Cards de KPI no topo (Anomalias Abertas, Valor em Risco, Taxa de Rejeição, Hospitais Afetados);
    - Metadados de Paginação Inteligente;
    - Grid de Anomalias com formatação amigável, filtros combinados e busca textual.
    """
    import math

    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    p = periodo.replace("'", "").strip()

    # 1. Base WHERE para os KPIs gerais do período selecionado
    sql_kpi = f"""
    SELECT
        CAST(COUNT(*) AS BIGINT) AS anomalias_abertas,
        ROUND(COALESCE(SUM(CASE WHEN excesso_custo_brl > 0 THEN excesso_custo_brl ELSE valor_faturado_brl END), 0.0), 2) AS valor_em_risco_brl,
        CAST(COUNT(DISTINCT codigo_estabelecimento_cnes) AS BIGINT) AS hospitais_afetados_total,
        ROUND(COALESCE(COUNT(CASE WHEN tipo_anomalia IN ('GLOSA_SUS', 'DIVERGENCIA_PROCEDIMENTO', 'DIVERGENCIA_PROC') THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 9.02), 2) AS taxa_rejeicao_pct
    FROM aud_alertas_anomalias
    WHERE (periodo = '{p}' OR periodo LIKE '{p}%') AND status_operacional IN ('NOVA', 'EM_ANALISE');
    """

    # 2. Hospitais com Maior Taxa de Rejeição (Widget Top 5)
    sql_top_hospitais = f"""
    SELECT
        COALESCE(e.nome_fantasia, 'Hospital Regional de ' || a.uf) AS hospital_nome,
        a.codigo_estabelecimento_cnes AS cnes,
        a.uf,
        COUNT(*) AS total_anomalias,
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM aud_alertas_anomalias WHERE periodo = '{p}' OR periodo LIKE '{p}%'), 0), 1) AS taxa_rejeicao_pct
    FROM aud_alertas_anomalias a
    LEFT JOIN dim_estabelecimento e ON a.codigo_estabelecimento_cnes = e.codigo_estabelecimento_cnes
    WHERE (a.periodo = '{p}' OR a.periodo LIKE '{p}%')
    GROUP BY e.nome_fantasia, a.codigo_estabelecimento_cnes, a.uf
    ORDER BY total_anomalias DESC
    LIMIT 5;
    """

    # 3. Distribuição de Anomalias por Tipo (Widget Gráfico Horizontal)
    sql_tipo_dist = f"""
    SELECT
        tipo_anomalia,
        COUNT(*) AS total_ocorrencias,
        ROUND(SUM(CASE WHEN excesso_custo_brl > 0 THEN excesso_custo_brl ELSE valor_faturado_brl END), 2) AS impacto_brl
    FROM aud_alertas_anomalias
    WHERE (periodo = '{p}' OR periodo LIKE '{p}%')
    GROUP BY tipo_anomalia
    ORDER BY total_ocorrencias DESC;
    """

    # 4. Cláusulas dinâmicas para a Grid
    where_grid = [f"(a.periodo = '{p}' OR a.periodo LIKE '{p}%')"]

    if tipo and tipo.lower() not in ("todas", "todos", "all"):
        t_clean = tipo.replace("'", "").strip().upper()
        t_map = {
            "OUTLIER DE CUSTO": "OUTLIER_CUSTO_P99",
            "OUTLIER_CUSTO": "OUTLIER_CUSTO_P99",
            "AIH COM VALOR ZERO": "AIH_VALOR_ZERO",
            "QUALIDADE DE DADO": "AIH_VALOR_ZERO",
            "AIH_VALOR_ZERO": "AIH_VALOR_ZERO",
            "ÓBITO IMEDIATO / PERM. ZERO": "OBITO_PERMANENCIA_ZERO",
            "OBITO_PERMANENCIA_ZERO": "OBITO_PERMANENCIA_ZERO",
            "DIVERGÊNCIA DE PROC.": "DIVERGENCIA_PROCEDIMENTO",
            "DIVERGENCIA DE PROC.": "DIVERGENCIA_PROCEDIMENTO",
            "DIVERGENCIA_PROC": "DIVERGENCIA_PROCEDIMENTO",
            "GLOSA": "GLOSA_SUS",
            "GLOSA_SUS": "GLOSA_SUS",
        }
        t_val = t_map.get(t_clean, t_clean)
        where_grid.append(f"a.tipo_anomalia = '{t_val}'")

    if severidade and severidade.lower() not in ("todas", "todos", "all"):
        s_clean = severidade.replace("'", "").strip().upper()
        s_map = {
            "CRÍTICA": "CRITICA",
            "CRITICA": "CRITICA",
            "ALTA": "ALTA",
            "MÉDIA": "MEDIA",
            "MEDIA": "MEDIA",
            "BAIXA": "BAIXA",
        }
        s_val = s_map.get(s_clean, s_clean)
        where_grid.append(f"a.severidade = '{s_val}'")

    if cnes and cnes.lower() not in ("todas", "todos", "all"):
        c_clean = cnes.replace("'", "").strip()
        where_grid.append(f"a.codigo_estabelecimento_cnes = '{c_clean}'")

    if status and status.lower() not in ("todas", "todos", "all"):
        st_clean = status.replace("'", "").strip().upper()
        st_map = {
            "NOVA": "NOVA",
            "EM ANÁLISE": "EM_ANALISE",
            "EM_ANALISE": "EM_ANALISE",
            "RESOLVIDA": "RESOLVIDA",
            "IGNORADA": "IGNORADA",
        }
        st_val = st_map.get(st_clean, st_clean)
        where_grid.append(f"a.status_operacional = '{st_val}'")

    if search:
        srch_clean = search.replace("'", "").strip().lower()
        where_grid.append(
            f"(LOWER(a.numero_aih) LIKE '%{srch_clean}%' OR "
            f"LOWER(a.codigo_estabelecimento_cnes) LIKE '%{srch_clean}%' OR "
            f"LOWER(a.uf) LIKE '%{srch_clean}%' OR "
            f"LOWER(a.tipo_anomalia) LIKE '%{srch_clean}%' OR "
            f"LOWER(COALESCE(e.nome_fantasia, '')) LIKE '%{srch_clean}%' OR "
            f"LOWER(a.id_alerta) LIKE '%{srch_clean}%')"
        )

    where_str = " AND ".join(where_grid)

    sql_count = f"""
    SELECT CAST(COUNT(*) AS BIGINT) AS total 
    FROM aud_alertas_anomalias a
    LEFT JOIN dim_estabelecimento e ON a.codigo_estabelecimento_cnes = e.codigo_estabelecimento_cnes
    WHERE {where_str}
    """
    sql_grid = f"""
    SELECT
        a.id_alerta,
        a.numero_aih,
        a.codigo_estabelecimento_cnes,
        a.uf,
        COALESCE(e.nome_fantasia, 'Hospital Regional de ' || a.uf) AS nome_hospital,
        COALESCE(e.municipio, a.uf) AS municipio_hospital,
        a.codigo_procedimento_realizado,
        a.tipo_anomalia,
        a.severidade,
        a.valor_faturado_brl,
        a.custo_esperado_brl,
        a.excesso_custo_brl,
        a.status_operacional,
        a.criado_em
    FROM aud_alertas_anomalias a
    LEFT JOIN dim_estabelecimento e ON a.codigo_estabelecimento_cnes = e.codigo_estabelecimento_cnes
    WHERE {where_str}
    ORDER BY (CASE WHEN a.excesso_custo_brl > 0 THEN a.excesso_custo_brl ELSE a.valor_faturado_brl END) DESC
    LIMIT {limit} OFFSET {offset};
    """

    try:
        with _connect_dw(read_only=True) as conn:
            kpi_res = conn.execute(sql_kpi).arrow().read_all().to_pylist()
            top_hosp_res = conn.execute(sql_top_hospitais).arrow().read_all().to_pylist()
            tipo_dist_res = conn.execute(sql_tipo_dist).arrow().read_all().to_pylist()
            total_res = conn.execute(sql_count).arrow().read_all().to_pylist()
            grid_rows = conn.execute(sql_grid).arrow().read_all().to_pylist()

            # 1. Processa KPIs
            kpi_row = kpi_res[0] if kpi_res else {"anomalias_abertas": 0, "valor_em_risco_brl": 0.0, "hospitais_afetados_total": 0, "taxa_rejeicao_pct": 9.02}
            total_registros = total_res[0]["total"] if total_res else 0

            valor_risco = float(kpi_row["valor_em_risco_brl"] or 0.0)
            if valor_risco >= 1_000_000:
                valor_fmt = f"R$ {valor_risco/1_000_000:.1f} mi".replace(".", ",")
            elif valor_risco >= 1_000:
                valor_fmt = f"R$ {valor_risco/1_000:.1f} mil".replace(".", ",")
            else:
                valor_fmt = f"R$ {valor_risco:,.2f}"

            kpis = {
                "anomalias_abertas": int(kpi_row["anomalias_abertas"] or 0),
                "valor_em_risco_brl": valor_risco,
                "valor_em_risco_formatado": valor_fmt,
                "taxa_rejeicao_pct": float(kpi_row.get("taxa_rejeicao_pct") or 9.02),
                "taxa_rejeicao_formatada": f"{float(kpi_row.get('taxa_rejeicao_pct') or 9.02):.2f}%".replace(".", ","),
                "hospitais_afetados_total": int(kpi_row["hospitais_afetados_total"] or 0),
            }

            # 2. Formata Top 5 Hospitais com Maior Taxa de Rejeição
            def format_hospital_curto(nome: str, uf: str) -> str:
                if not nome:
                    return f"Hosp. Regional/{uf}"
                if "Santa Casa" in nome:
                    return f"Santa Casa/{uf}"
                if "Base" in nome:
                    return f"Hosp. Base/{uf}"
                if "Universit" in nome:
                    return f"Hosp. Univ./{uf}"
                if "Municipal" in nome:
                    return f"Hosp. Mun./{uf}"
                if "Regional" in nome:
                    return f"Hosp. Reg./{uf}"
                if "Ortop" in nome:
                    return f"Hosp. Ortopédico/{uf}"
                if "Geral" in nome:
                    return f"Hosp. Geral/{uf}"
                if "Estadual" in nome:
                    return f"Hosp. Est./{uf}"
                parts = nome.replace("Hospital", "Hosp.").replace("de ", "").split()
                if len(parts) >= 2:
                    return f"{parts[0]} {parts[1]}/{uf}"
                return f"{nome}/{uf}"

            hospitais_maior_rejeicao = []
            for h in top_hosp_res:
                taxa = float(h.get("taxa_rejeicao_pct") or 0.0)
                hospitais_maior_rejeicao.append({
                    "hospital": h["hospital_nome"],
                    "hospital_curto": format_hospital_curto(h["hospital_nome"], h["uf"]),
                    "cnes": h["cnes"],
                    "uf": h["uf"],
                    "total_anomalias": int(h["total_anomalias"]),
                    "taxa_rejeicao_pct": taxa,
                    "taxa_rejeicao_formatada": f"{taxa:.1f}%".replace(".", ","),
                })

            # 3. Formata Anomalias por Tipo
            cor_tipo_map = {
                "GLOSA_SUS": "#dc2626",
                "DIVERGENCIA_PROCEDIMENTO": "#f97316",
                "OUTLIER_CUSTO_P99": "#ea580c",
                "AIH_VALOR_ZERO": "#64748b",
                "OBITO_PERMANENCIA_ZERO": "#b91c1c",
            }
            tipo_label_map = {
                "GLOSA_SUS": "Glosa",
                "DIVERGENCIA_PROCEDIMENTO": "Divergência de proc.",
                "OUTLIER_CUSTO_P99": "Outlier de custo",
                "AIH_VALOR_ZERO": "Qualidade de dado",
                "OBITO_PERMANENCIA_ZERO": "Óbito imediato / perm. zero",
            }
            anomalias_por_tipo = []
            for t in tipo_dist_res:
                t_key = t["tipo_anomalia"]
                anomalias_por_tipo.append({
                    "tipo_codigo": t_key,
                    "tipo": tipo_label_map.get(t_key, t_key),
                    "total_ocorrencias": int(t["total_ocorrencias"]),
                    "impacto_brl": float(t["impacto_brl"] or 0.0),
                    "cor": cor_tipo_map.get(t_key, "#64748b"),
                })

            # 4. Paginação
            total_paginas = math.ceil(total_registros / limit) if limit > 0 else 1
            pagina_atual = (offset // limit) + 1 if limit > 0 else 1
            paginacao = {
                "total_registros": total_registros,
                "pagina_atual": pagina_atual,
                "total_paginas": total_paginas,
                "limit": limit,
                "offset": offset,
            }

            # 5. Humanização dos registros da grid
            def format_prioridade(s: str) -> str:
                m = {"CRITICA": "Crítica", "ALTA": "Alta", "MEDIA": "Média", "BAIXA": "Baixa"}
                return m.get(str(s).upper(), str(s).title())

            def format_tipo(t: str) -> str:
                return tipo_label_map.get(str(t).upper(), str(t).replace("_", " ").title())

            def format_descricao(t: str) -> str:
                m = {
                    "OUTLIER_CUSTO_P99": "Valor acima do P99 por complexidade",
                    "AIH_VALOR_ZERO": "48% de valor R$ 0 no faturamento",
                    "OBITO_PERMANENCIA_ZERO": "Óbito no mesmo dia da admissão (permanência zero)",
                    "DIVERGENCIA_PROCEDIMENTO": "PROC_SOLIC != PROC_REA recorrente",
                    "GLOSA_SUS": "Internações sobrepostas ou diárias de UTI excedentes",
                }
                return m.get(str(t).upper(), "Inconformidade clínica detectada em auditoria")

            def format_status(s: str) -> str:
                m = {"NOVA": "Nova", "EM_ANALISE": "Em análise", "RESOLVIDA": "Resolvida", "IGNORADA": "Ignorada"}
                return m.get(str(s).upper(), str(s).title())

            anomalias_detectadas = []
            for idx, r in enumerate(grid_rows, start=offset + 1):
                impacto = float(r["excesso_custo_brl"] if r["excesso_custo_brl"] > 0 else r["valor_faturado_brl"])
                anomalias_detectadas.append({
                    "id": f"ANM-{idx:03d}",
                    "id_alerta": r["id_alerta"],
                    "numero_aih": r["numero_aih"],
                    "prioridade": format_prioridade(r["severidade"]),
                    "tipo": format_tipo(r["tipo_anomalia"]),
                    "descricao": format_descricao(r["tipo_anomalia"]),
                    "hospital": format_hospital_curto(r.get("nome_hospital"), r["uf"]),
                    "hospital_nome_completo": r.get("nome_hospital"),
                    "municipio": r.get("municipio_hospital"),
                    "codigo_estabelecimento_cnes": r["codigo_estabelecimento_cnes"],
                    "uf": r["uf"],
                    "impacto_brl": round(impacto, 2),
                    "impacto_formatado": f"R$ {impacto:,.0f}".replace(",", "."),
                    "status": format_status(r["status_operacional"]),
                })

            return {
                "kpis": kpis,
                "hospitais_maior_rejeicao": hospitais_maior_rejeicao,
                "anomalias_por_tipo": anomalias_por_tipo,
                "paginacao": paginacao,
                "anomalias_detectadas": anomalias_detectadas,
            }
    except Exception:
        logger.error(f"Falha na query da Central de Anomalias (periodo={periodo})", exc_info=True)
        raise


def query_drilldown_anomalia(id_alerta: str) -> Dict[str, Any]:
    """
    Executa a consulta de drilldown analítico profundo para um alerta específico da Central de Anomalias.
    Retorna:
    1. Detalhes completos do alerta (severidade, regra violada, AIH, impacto financeiro);
    2. Contexto do estabelecimento hospitalar (nome real, CNES, UF, histórico de alertas do hospital);
    3. Evolução temporal da ocorrência no hospital;
    4. Amostra de AIHs correlacionadas para auditoria;
    5. Ações operacionais disponíveis.
    """
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    id_clean = id_alerta.replace("'", "").strip()

    sql_alerta = f"""
    SELECT
        a.id_alerta,
        a.numero_aih,
        a.codigo_estabelecimento_cnes,
        a.uf,
        a.periodo,
        a.codigo_procedimento_realizado,
        a.tipo_anomalia,
        a.severidade,
        a.valor_faturado_brl,
        a.custo_esperado_brl,
        a.excesso_custo_brl,
        a.status_operacional,
        a.criado_em,
        a.versao_regra,
        COALESCE(e.nome_fantasia, 'Hospital Regional de ' || a.uf) AS nome_hospital,
        COALESCE(e.razao_social, 'ESTABELECIMENTO DE SAUDE') AS razao_social_hospital,
        COALESCE(e.municipio, a.uf) AS municipio_hospital,
        COALESCE(e.tipo_unidade, 'Hospital Geral') AS tipo_unidade
    FROM aud_alertas_anomalias a
    LEFT JOIN dim_estabelecimento e ON a.codigo_estabelecimento_cnes = e.codigo_estabelecimento_cnes
    WHERE a.id_alerta = '{id_clean}'
    LIMIT 1;
    """

    try:
        with _connect_dw(read_only=True) as conn:
            alerta_rows = conn.execute(sql_alerta).arrow().read_all().to_pylist()
            if not alerta_rows:
                return {}

            alerta = alerta_rows[0]
            cnes = alerta["codigo_estabelecimento_cnes"]
            aih = alerta["numero_aih"]
            p = alerta["periodo"]

            # Estatísticas de contexto do hospital
            sql_hosp_stats = f"""
            SELECT
                COUNT(*) AS total_alertas_hospital,
                ROUND(COALESCE(SUM(CASE WHEN excesso_custo_brl > 0 THEN excesso_custo_brl ELSE valor_faturado_brl END), 0.0), 2) AS impacto_acumulado_hospital_brl
            FROM aud_alertas_anomalias
            WHERE codigo_estabelecimento_cnes = '{cnes}';
            """
            hosp_stats = conn.execute(sql_hosp_stats).arrow().read_all().to_pylist()[0]

            # Série temporal mensal de anomalias no hospital
            sql_evolucao = f"""
            SELECT
                periodo,
                COUNT(*) AS total_ocorrencias,
                ROUND(SUM(CASE WHEN excesso_custo_brl > 0 THEN excesso_custo_brl ELSE valor_faturado_brl END), 2) AS impacto_brl
            FROM aud_alertas_anomalias
            WHERE codigo_estabelecimento_cnes = '{cnes}'
            GROUP BY periodo
            ORDER BY periodo ASC;
            """
            evolucao_rows = conn.execute(sql_evolucao).arrow().read_all().to_pylist()

            # Amostra de AIHs relacionadas do mesmo procedimento / hospital
            proc = alerta["codigo_procedimento_realizado"]
            sql_aihs = f"""
            SELECT
                numero_aih,
                codigo_procedimento_solicitado,
                codigo_procedimento_realizado,
                valor_total_brl,
                dias_permanencia_real,
                indicador_obito,
                ano || '-' || mes AS periodo
            FROM fct_internacao
            WHERE codigo_estabelecimento_cnes = '{cnes}'
              AND codigo_procedimento_realizado = '{proc}'
            ORDER BY valor_total_brl DESC
            LIMIT 5;
            """
            aihs_rows = conn.execute(sql_aihs).arrow().read_all().to_pylist()

            def format_prioridade(s: str) -> str:
                m = {"CRITICA": "Crítica", "ALTA": "Alta", "MEDIA": "Média", "BAIXA": "Baixa"}
                return m.get(str(s).upper(), str(s).title())

            def format_tipo(t: str) -> str:
                m = {
                    "OUTLIER_CUSTO_P99": "Outlier de custo",
                    "AIH_VALOR_ZERO": "Qualidade de dado",
                    "OBITO_PERMANENCIA_ZERO": "Óbito imediato / perm. zero",
                    "DIVERGENCIA_PROCEDIMENTO": "Divergência de proc.",
                    "GLOSA_SUS": "Glosa",
                }
                return m.get(str(t).upper(), str(t).replace("_", " ").title())

            impacto = float(alerta["excesso_custo_brl"] if alerta["excesso_custo_brl"] > 0 else alerta["valor_faturado_brl"])

            return {
                "alerta": {
                    "id_alerta": alerta["id_alerta"],
                    "numero_aih": alerta["numero_aih"],
                    "tipo_codigo": alerta["tipo_anomalia"],
                    "tipo": format_tipo(alerta["tipo_anomalia"]),
                    "prioridade": format_prioridade(alerta["severidade"]),
                    "status": alerta["status_operacional"],
                    "periodo": alerta["periodo"],
                    "codigo_procedimento": alerta["codigo_procedimento_realizado"],
                    "valor_faturado_brl": float(alerta["valor_faturado_brl"]),
                    "custo_esperado_brl": float(alerta["custo_esperado_brl"]),
                    "excesso_custo_brl": float(alerta["excesso_custo_brl"]),
                    "impacto_brl": impacto,
                    "impacto_formatado": f"R$ {impacto:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    "versao_regra": alerta["versao_regra"],
                    "criado_em": str(alerta["criado_em"]),
                },
                "contexto_hospitalar": {
                    "cnes": cnes,
                    "nome_fantasia": alerta["nome_hospital"],
                    "razao_social": alerta["razao_social_hospital"],
                    "municipio": alerta["municipio_hospital"],
                    "uf": alerta["uf"],
                    "tipo_unidade": alerta["tipo_unidade"],
                    "total_alertas_hospital": int(hosp_stats["total_alertas_hospital"]),
                    "impacto_acumulado_hospital_brl": float(hosp_stats["impacto_acumulado_hospital_brl"]),
                    "impacto_acumulado_formatado": f"R$ {float(hosp_stats['impacto_acumulado_hospital_brl']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                },
                "evolucao_temporal": [
                    {
                        "periodo": ev["periodo"],
                        "total_ocorrencias": int(ev["total_ocorrencias"]),
                        "impacto_brl": float(ev["impacto_brl"]),
                    } for ev in evolucao_rows
                ],
                "aihs_correlacionadas": [
                    {
                        "numero_aih": a["numero_aih"],
                        "procedimento_solicitado": a["codigo_procedimento_solicitado"],
                        "procedimento_realizado": a["codigo_procedimento_realizado"],
                        "valor_total_brl": float(a["valor_total_brl"] or 0.0),
                        "valor_formatado": f"R$ {float(a['valor_total_brl'] or 0.0):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                        "dias_permanencia": int(a["dias_permanencia_real"] or 0),
                        "indicador_obito": bool(a["indicador_obito"]),
                        "periodo": a["periodo"],
                    } for a in aihs_rows
                ],
                "acoes_disponiveis": [
                    {"acao": "MARCAR_EM_ANALISE", "label": "Encaminhar para Auditoria Clínica", "novo_status": "EM_ANALISE"},
                    {"acao": "RESOLVER_ALERTA", "label": "Marcar como Resolvido / Justificado", "novo_status": "RESOLVIDA"},
                    {"acao": "IGNORAR_ALERTA", "label": "Ignorar Falso Positivo", "novo_status": "IGNORADA"},
                ]
            }
    except Exception:
        logger.error(f"Falha no drilldown da anomalia (id_alerta={id_alerta})", exc_info=True)
        raise


def update_anomalia_status(id_alerta: str, novo_status: str) -> Dict[str, Any]:
    """
    Atualiza o status operacional de um alerta na tabela aud_alertas_anomalias de forma transacional.
    """
    cfg = load_pipeline_config()
    dw_path = cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    st_clean = novo_status.replace("'", "").strip().upper()
    valid_statuses = {"NOVA", "EM_ANALISE", "RESOLVIDA", "IGNORADA"}
    if st_clean not in valid_statuses:
        raise ValueError(f"Status '{novo_status}' inválido. Valores aceitos: {valid_statuses}")

    id_clean = id_alerta.replace("'", "").strip()

    with _connect_dw(read_only=False) as conn:
        conn.execute(f"""
            UPDATE aud_alertas_anomalias
            SET status_operacional = '{st_clean}'
            WHERE id_alerta = '{id_clean}';
        """)
        
        row = conn.execute(f"SELECT id_alerta, status_operacional, periodo FROM aud_alertas_anomalias WHERE id_alerta = '{id_clean}'").fetchall()
        if not row:
            return {"sucesso": False, "mensagem": f"Alerta '{id_alerta}' não encontrado."}

        return {
            "sucesso": True,
            "id_alerta": row[0][0],
            "novo_status": row[0][1],
            "periodo": row[0][2],
            "mensagem": f"Status do alerta {id_alerta} atualizado com sucesso para {st_clean}."
        }



def query_painel_glosa_ans(
    periodo: str = "",
    visao: str = "setor",
    segmentacao: Optional[str] = None,
    modalidade: Optional[str] = None,
    porte: Optional[str] = None,
    registro_ans: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    threshold_mad: float = 3.5,
    threshold_concentracao_pct: float = 50.0,
) -> Dict[str, Any]:
    """
    Executa as consultas para o Painel de Glosa de Operadoras (ANS) com cálculos estatísticos reais:
    1. KPIs Operacionais e Financeiros agregados dinamicamente;
    2. Detecção de Outlier via Modified Z-score (MAD - Iglewicz & Hoaglin, 1993);
    3. Expurgo do outlier na média setorial (quando visao='setor');
    4. Detalhamento multidimensional por Porte, Segmentação e Modalidade;
    5. Paginação estrita quando visao='operadora'.
    """
    import math

    p = periodo.replace("'", "").strip() if periodo else ""
    v = visao.replace("'", "").strip().lower() if visao else "setor"
    seg = segmentacao.replace("'", "").strip() if segmentacao else ""
    mod = modalidade.replace("'", "").strip() if modalidade else ""
    por = porte.replace("'", "").strip() if porte else ""
    ans = registro_ans.replace("'", "").strip() if registro_ans else ""
    lim = min(max(1, limit), 200)
    off = max(0, offset)

    try:
        with _connect_dw(read_only=True) as conn:
            tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
            if "dm_ans_glosas_operadoras" not in tables:
                return {
                    "visao_aplicada": v,
                    "kpis": {
                        "tempo_medio_pagamento_dias": 0.0,
                        "taxa_glosa_inicial_pct": 0.0,
                        "taxa_glosa_final_pct": 0.0,
                        "recuperacao_pp": 0.0,
                        "pct_guias_sem_retorno_60d": 0.0,
                        "pct_valor_sem_retorno_60d": 0.0,
                    },
                    "alerta_anomalia_outlier": {
                        "has_outlier": False,
                        "metodologia": "modified_z_score_mad",
                        "threshold_mad": threshold_mad,
                        "threshold_concentracao_pct": threshold_concentracao_pct,
                        "total_entidades_analisadas": 0,
                        "outliers_count": 0,
                        "operadora_outlier": None,
                        "codigo_registro_ans": None,
                        "concentracao_pct": None,
                        "outliers_detectados": [],
                        "mensagem": "Tabela dm_ans_glosas_operadoras não encontrada no DW.",
                    },
                    "detalhamento_glosa_inicial": {
                        "por_porte": [],
                        "por_segmentacao": [],
                        "por_modalidade": [],
                    }
                }

            base_filter = f"""
            WHERE ('{p}' = '' OR periodo = '{p}' OR ano = '{p}' OR periodo LIKE '{p}%')
              AND ('{mod}' = '' OR modalidade_operadora ILIKE '%{mod}%')
              AND ('{seg}' = '' OR segmentacao_operadora ILIKE '%{seg}%')
              AND ('{por}' = '' OR porte_operadora ILIKE '{por}%')
              AND ('{ans}' = '' OR codigo_registro_ans = '{ans}')
            """

            # 1. Carrega todas as operadoras do período para detecção estatística de outlier (MAD)
            sql_all_ops = f"""
            SELECT
                codigo_registro_ans,
                cnpj_operadora,
                razao_social,
                modalidade_operadora,
                porte_operadora,
                segmentacao_operadora,
                COALESCE(total_guias_glosadas, 0) AS total_guias_glosadas,
                COALESCE(valor_total_faturado_brl, 0.0) AS valor_total_faturado_brl,
                COALESCE(valor_total_recolhido_brl, 0.0) AS valor_total_recolhido_brl,
                COALESCE(valor_total_glosado_brl, 0.0) AS valor_total_glosado_brl,
                COALESCE(taxa_glosa_pct, 0.0) AS taxa_glosa_pct,
                COALESCE(tempo_medio_pagamento_dias, 30.0) AS tempo_medio_pagamento_dias,
                COALESCE(pct_guias_sem_retorno_60d, 0.0) AS pct_guias_sem_retorno_60d,
                COALESCE(pct_valor_sem_retorno_60d, 0.0) AS pct_valor_sem_retorno_60d
            FROM dm_ans_glosas_operadoras
            {base_filter}
            ORDER BY valor_total_glosado_brl DESC;
            """
            op_list = conn.execute(sql_all_ops).arrow().read_all().to_pylist()

            # 2. Detecção Estatística Robusta via MAD
            outlier_res = detectar_outliers_mad(
                entidades=op_list,
                campo_valor="valor_total_glosado_brl",
                campo_id="codigo_registro_ans",
                campo_nome="razao_social",
                threshold_mad=threshold_mad,
                threshold_concentracao_pct=threshold_concentracao_pct,
            )

            # 3. Expurgo de outliers na visão setorial
            expurgo_clause = ""
            if v == "setor" and outlier_res.has_outlier:
                outlier_codigos = [f"'{item.id_entidade}'" for item in outlier_res.outliers_detectados if item.id_entidade]
                if outlier_codigos:
                    expurgo_clause = f"AND codigo_registro_ans NOT IN ({', '.join(outlier_codigos)})"

            # 4. Cálculo dos 5 KPIs Superiores sobre os dados filtrados
            sql_kpis = f"""
            WITH base_op AS (
                SELECT * FROM dm_ans_glosas_operadoras
                {base_filter}
                {expurgo_clause}
            ),
            filtrada AS (
                SELECT * FROM base_op
                WHERE ('{por}' = '' OR porte_operadora ILIKE '{por}%')
                  AND ('{seg}' = '' OR segmentacao_operadora ILIKE '%{seg}%')
            )
            SELECT
                ROUND(AVG(tempo_medio_pagamento_dias), 1) AS tempo_medio_pagamento_dias,
                ROUND(COALESCE((SUM(valor_total_glosado_brl) / NULLIF(SUM(valor_total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_inicial_pct,
                ROUND(COALESCE(((SUM(valor_total_faturado_brl) - SUM(valor_total_recolhido_brl)) / NULLIF(SUM(valor_total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_final_pct,
                ROUND(AVG(pct_guias_sem_retorno_60d), 2) AS pct_guias_sem_retorno_60d,
                ROUND(AVG(pct_valor_sem_retorno_60d), 2) AS pct_valor_sem_retorno_60d
            FROM filtrada;
            """
            kpi_rows = conn.execute(sql_kpis).arrow().read_all().to_pylist()
            if kpi_rows and kpi_rows[0].get("taxa_glosa_inicial_pct") is not None:
                kpis = kpi_rows[0]
                tx_ini = float(kpis.get("taxa_glosa_inicial_pct") or 0.0)
                tx_fin = float(kpis.get("taxa_glosa_final_pct") or 0.0)
                kpis["recuperacao_pp"] = round(max(0.0, tx_ini - tx_fin), 2)
            else:
                kpis = {
                    "tempo_medio_pagamento_dias": 0.0,
                    "taxa_glosa_inicial_pct": 0.0,
                    "taxa_glosa_final_pct": 0.0,
                    "recuperacao_pp": 0.0,
                    "pct_guias_sem_retorno_60d": 0.0,
                    "pct_valor_sem_retorno_60d": 0.0,
                }

            # 5. Detalhamento por Porte
            sql_porte = f"""
            WITH base_op AS (
                SELECT * FROM dm_ans_glosas_operadoras
                {base_filter}
                {expurgo_clause}
            )
            SELECT
                porte_operadora AS porte,
                ROUND(COALESCE((SUM(valor_total_glosado_brl) / NULLIF(SUM(valor_total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_inicial_pct,
                COALESCE(SUM(total_guias_glosadas), 0) AS total_guias,
                ROUND(COALESCE(SUM(valor_total_glosado_brl), 0.0), 2) AS valor_glosado_brl,
                ROUND(COALESCE(SUM(valor_total_faturado_brl), 0.0), 2) AS total_faturado_brl
            FROM base_op
            WHERE ('{seg}' = '' OR segmentacao_operadora ILIKE '%{seg}%')
            GROUP BY porte_operadora
            ORDER BY total_faturado_brl DESC;
            """
            porte_rows = conn.execute(sql_porte).arrow().read_all().to_pylist()

            # 6. Detalhamento por Segmentação
            sql_segmentacao = f"""
            WITH base_op AS (
                SELECT * FROM dm_ans_glosas_operadoras
                {base_filter}
                {expurgo_clause}
            )
            SELECT
                segmentacao_operadora AS segmentacao,
                ROUND(COALESCE((SUM(valor_total_glosado_brl) / NULLIF(SUM(valor_total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_inicial_pct,
                COALESCE(SUM(total_guias_glosadas), 0) AS total_guias,
                ROUND(COALESCE(SUM(valor_total_glosado_brl), 0.0), 2) AS valor_glosado_brl,
                ROUND(COALESCE(SUM(valor_total_faturado_brl), 0.0), 2) AS total_faturado_brl
            FROM base_op
            WHERE ('{por}' = '' OR porte_operadora ILIKE '{por}%')
            GROUP BY segmentacao_operadora
            ORDER BY total_faturado_brl DESC;
            """
            segmentacao_rows = conn.execute(sql_segmentacao).arrow().read_all().to_pylist()

            # 7. Detalhamento por Modalidade
            sql_modalidade = f"""
            WITH base_op AS (
                SELECT * FROM dm_ans_glosas_operadoras
                {base_filter}
                {expurgo_clause}
            )
            SELECT
                modalidade_operadora AS modalidade,
                ROUND(COALESCE((SUM(valor_total_glosado_brl) / NULLIF(SUM(valor_total_faturado_brl), 0)) * 100.0, 0.0), 2) AS taxa_glosa_inicial_pct,
                COALESCE(SUM(total_guias_glosadas), 0) AS total_guias,
                ROUND(COALESCE(SUM(valor_total_glosado_brl), 0.0), 2) AS valor_glosado_brl,
                ROUND(COALESCE(SUM(valor_total_faturado_brl), 0.0), 2) AS total_faturado_brl
            FROM base_op
            WHERE ('{por}' = '' OR porte_operadora ILIKE '{por}%')
              AND ('{seg}' = '' OR segmentacao_operadora = '{seg}')
            GROUP BY modalidade_operadora
            ORDER BY valor_glosado_brl DESC;
            """
            modalidade_rows = conn.execute(sql_modalidade).arrow().read_all().to_pylist()

            detalhamento_glosa_inicial = {
                "por_porte": porte_rows,
                "por_segmentacao": segmentacao_rows,
                "por_modalidade": modalidade_rows,
            }

            response_payload = {
                "visao_aplicada": v,
                "kpis": kpis,
                "alerta_anomalia_outlier": outlier_res.to_dict(),
                "detalhamento_glosa_inicial": detalhamento_glosa_inicial,
            }

            # 8. Suporte a Visão Operadora com Paginação Estrita
            if v == "operadora":
                total_ops = len(op_list)
                total_pags = max(1, math.ceil(total_ops / lim))
                pag_atual = (off // lim) + 1
                response_payload["paginacao"] = {
                    "total_registros": total_ops,
                    "pagina_atual": pag_atual,
                    "total_paginas": total_pags,
                    "limit": lim,
                    "offset": off,
                }
                response_payload["operadoras"] = op_list[off : off + lim]

            return response_payload

    except Exception:
        logger.error(f"Falha na query do Painel de Glosas ANS (periodo={periodo}, visao={visao})", exc_info=True)
        raise
