"""
Gold Pipeline - Data Marts Nativos em DuckDB - QIMED Lakehouse V3.
Gera os Data Marts e indicadores anal?ticos diretamente no DuckDB Data Warehouse
a partir das tabelas can?nicas da Camada Silver (Delta Lake) via SQL vetorizado out-of-core.
"""
import os
import duckdb
from typing import Any, Dict, List, Optional

from src.processing.duckdb_engine import DuckDBEngine
from src.processing.lineage_tracker import DataLineageTracker
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class GoldPipelineNacional:
    """
    Consolida as tabelas agregadas Gold (Data Marts) 100% em portugu?s diretamente no DuckDB DW.
    """

    def __init__(self, dw_file_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        base_warehouse = dw_file_path or self.cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")
        os.makedirs(os.path.dirname(base_warehouse), exist_ok=True)
        self.dw_file_path = base_warehouse
        self.silver_dir = self.cfg.get("paths", {}).get("silver_dir", "lakehouse/silver")
        self.lineage_tracker = DataLineageTracker()

    def build_gold_data_marts(self, execution_id: str = "exec_gold") -> Dict[str, Any]:
        """
        Gera e materializa todos os Data Marts anal?ticos no arquivo DuckDB DW.
        """
        conn = duckdb.connect(self.dw_file_path)
        try:
            try:
                conn.execute("INSTALL delta; LOAD delta;")
            except Exception:
                pass

            fct_int_path = os.path.join(self.silver_dir, "fct_internacao").replace(chr(92), "/")
            fct_amb_path = os.path.join(self.silver_dir, "fct_atendimentos_ambulatoriais").replace(chr(92), "/")

            # 1. agg_internacoes_uf
            if os.path.exists(fct_int_path):
                sql_int_uf = f"""
                CREATE OR REPLACE TABLE agg_internacoes_uf AS
                SELECT
                    uf,
                    ano,
                    mes,
                    COUNT(*) AS total_internacoes,
                    SUM(COALESCE(dias_permanencia_real, 0)) AS total_dias_internacao,
                    ROUND(AVG(COALESCE(dias_permanencia_real, 0)), 2) AS media_dias_permanencia,
                    SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS total_obitos,
                    ROUND(SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS taxa_mortalidade_pct,
                    ROUND(SUM(COALESCE(valor_total_brl, 0.0)), 2) AS valor_total_brl,
                    ROUND(AVG(COALESCE(valor_total_brl, 0.0)), 2) AS valor_medio_internacao_brl
                FROM delta_scan('{fct_int_path}')
                GROUP BY uf, ano, mes
                ORDER BY uf;
                """
                conn.execute(sql_int_uf)
                self.lineage_tracker.record_lineage(execution_id, "Silver", "fct_internacao", "Gold", "agg_internacoes_uf", 27)
                logger.info("[GOLD] Data Mart agg_internacoes_uf materializado no DuckDB DW.")

            # 2. agg_procedimentos_uf
            if os.path.exists(fct_amb_path):
                sql_proc_uf = f"""
                CREATE OR REPLACE TABLE agg_procedimentos_uf AS
                SELECT
                    uf,
                    ano,
                    mes,
                    COUNT(*) AS total_atendimentos_ambulatoriais,
                    SUM(COALESCE(quantidade_produzida, 0)) AS total_procedimentos_produzidos,
                    SUM(COALESCE(quantidade_aprovada, 0)) AS total_procedimentos_aprovados,
                    ROUND(SUM(COALESCE(valor_produzido_brl, 0.0)), 2) AS valor_total_produzido_brl,
                    ROUND(SUM(COALESCE(valor_aprovado_brl, 0.0)), 2) AS valor_total_aprovado_brl,
                    ROUND(AVG(COALESCE(valor_aprovado_brl, 0.0)), 2) AS valor_medio_procedimento_brl
                FROM delta_scan('{fct_amb_path}')
                GROUP BY uf, ano, mes
                ORDER BY uf;
                """
                conn.execute(sql_proc_uf)
                self.lineage_tracker.record_lineage(execution_id, "Silver", "fct_atendimentos_ambulatoriais", "Gold", "agg_procedimentos_uf", 27)
                logger.info("[GOLD] Data Mart agg_procedimentos_uf materializado no DuckDB DW.")

            # 3. agg_perfil_epidemiologico
            if os.path.exists(fct_int_path):
                sql_epi = f"""
                CREATE OR REPLACE TABLE agg_perfil_epidemiologico AS
                SELECT
                    codigo_cid10_principal AS capitulo_cid10,
                    uf,
                    COUNT(*) AS total_internacoes,
                    SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS total_obitos,
                    ROUND(SUM(COALESCE(valor_total_brl, 0.0)), 2) AS custo_total_brl
                FROM delta_scan('{fct_int_path}')
                WHERE codigo_cid10_principal IS NOT NULL
                GROUP BY codigo_cid10_principal, uf
                ORDER BY total_internacoes DESC;
                """
                conn.execute(sql_epi)
                logger.info("[GOLD] Data Mart agg_perfil_epidemiologico materializado no DuckDB DW.")

            # 4. aud_alertas_anomalias (Central de Anomalias)
            if os.path.exists(fct_int_path):
                from src.gold.models.kpi_central_anomalias import build_aud_alertas_anomalias
                build_aud_alertas_anomalias(conn, fct_internacao_source=f"delta_scan('{fct_int_path}')", target_table="aud_alertas_anomalias")
                logger.info("[GOLD] Tabela de auditoria aud_alertas_anomalias materializada no DuckDB DW.")

            # 5. Views Semânticas Analíticas e Preditivas
            if os.path.exists(fct_int_path):
                from src.gold.models.views_semanticas import registrar_views_semanticas
                try:
                    conn.execute(f"CREATE OR REPLACE VIEW fct_internacao AS SELECT * FROM delta_scan('{fct_int_path}')")
                except Exception:
                    pass
                registrar_views_semanticas(conn)
                logger.info("[GOLD] Views semânticas registradas com sucesso no DuckDB DW.")

            tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
            return {"status": "success", "tables_created": tables, "dw_file": self.dw_file_path}
        finally:
            conn.close()
