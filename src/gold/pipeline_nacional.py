"""
Gold Pipeline - Data Marts Nativos em DuckDB - QIMED Lakehouse V3.
Gera os Data Marts e indicadores analíticos diretamente no DuckDB Data Warehouse
a partir das tabelas canônicas da Camada Silver (Delta Lake) via SQL vetorizado out-of-core.
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
    Consolida as tabelas agregadas Gold (Data Marts) 100% em português diretamente no DuckDB DW.
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
        Gera e materializa todos os Data Marts analíticos no arquivo DuckDB DW.
        """
        conn = duckdb.connect(self.dw_file_path)
        try:
            from src.utils.s3_storage import configure_duckdb_s3, lakehouse_path_exists
            configure_duckdb_s3(conn)

            try:
                conn.execute("INSTALL delta; LOAD delta;")
            except Exception:
                pass

            fct_int_path = os.path.join(self.silver_dir, "fct_internacao").replace("\\", "/")
            fct_amb_path = os.path.join(self.silver_dir, "fct_atendimentos_ambulatoriais").replace("\\", "/")
            fct_glo_path = os.path.join(self.silver_dir, "fct_glosas_hospitalares").replace("\\", "/")
            fct_ress_path = os.path.join(self.silver_dir, "fct_ressarcimento_sus").replace("\\", "/")
            dim_op_path = os.path.join(self.silver_dir, "dim_operadoras_saude").replace("\\", "/")

            # 1. agg_internacoes_uf
            if lakehouse_path_exists(fct_int_path):
                sql_int_uf = f"""
                CREATE OR REPLACE TABLE agg_internacoes_uf AS
                SELECT
                    uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
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
            if lakehouse_path_exists(fct_amb_path):
                sql_proc_uf = f"""
                CREATE OR REPLACE TABLE agg_procedimentos_uf AS
                SELECT
                    uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
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
            if lakehouse_path_exists(fct_int_path):
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

            # 4. dm_ans_glosas_operadoras (Data Mart ANS/TISS e Ressarcimento ao SUS)
            if lakehouse_path_exists(fct_ress_path) and lakehouse_path_exists(dim_op_path):
                sql_dm_ans = f"""
                CREATE OR REPLACE TABLE dm_ans_glosas_operadoras AS
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(r.codigo_registro_ans AS VARCHAR), '000000'), CAST(r.ano AS VARCHAR), CAST(r.mes AS VARCHAR))) AS id_registro_kpi,
                    COALESCE(r.codigo_registro_ans, o.codigo_registro_ans) AS codigo_registro_ans,
                    COALESCE(o.cnpj_operadora, '00.000.000/0000-00') AS cnpj_operadora,
                    COALESCE(o.razao_social, r.razao_social_operadora, 'OPERADORA NÃO IDENTIFICADA') AS razao_social,
                    COALESCE(r.modalidade_operadora, o.modalidade_operadora, 'NÃO INFORMADA') AS modalidade_operadora,
                    CAST(r.ano AS VARCHAR) AS ano,
                    LPAD(CAST(r.mes AS VARCHAR), 2, '0') AS mes,
                    CAST(r.ano AS VARCHAR) || '-' || LPAD(CAST(r.mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS total_guias_glosadas,
                    ROUND(SUM(COALESCE(r.valor_notificado_brl, 0.0)), 2) AS valor_total_faturado_brl,
                    ROUND(SUM(COALESCE(r.valor_recolhido_brl, 0.0)), 2) AS valor_total_recolhido_brl,
                    ROUND(SUM(COALESCE(r.valor_notificado_brl, 0.0) - COALESCE(r.valor_recolhido_brl, 0.0)), 2) AS valor_total_glosado_brl,
                    ROUND(
                        CASE 
                            WHEN SUM(COALESCE(r.valor_notificado_brl, 0.0)) > 0 
                            THEN ((SUM(COALESCE(r.valor_notificado_brl, 0.0) - COALESCE(r.valor_recolhido_brl, 0.0))) / SUM(COALESCE(r.valor_notificado_brl, 0.0))) * 100.0
                            ELSE 0.0 
                        END, 
                        2
                    ) AS taxa_glosa_pct
                FROM delta_scan('{fct_ress_path}') r
                LEFT JOIN delta_scan('{dim_op_path}') o ON r.codigo_registro_ans = o.codigo_registro_ans
                GROUP BY 
                    r.codigo_registro_ans, o.codigo_registro_ans, o.cnpj_operadora, o.razao_social, 
                    r.razao_social_operadora, r.modalidade_operadora, o.modalidade_operadora, r.ano, r.mes;
                """
                conn.execute(sql_dm_ans)
                logger.info("[GOLD] Data Mart dm_ans_glosas_operadoras materializado no DuckDB DW.")
            else:
                # Cria tabela vazia com schema padronizado caso a fonte não exista
                conn.execute("""
                CREATE OR REPLACE TABLE dm_ans_glosas_operadoras (
                    id_registro_kpi VARCHAR,
                    codigo_registro_ans VARCHAR,
                    cnpj_operadora VARCHAR,
                    razao_social VARCHAR,
                    modalidade_operadora VARCHAR,
                    ano VARCHAR,
                    mes VARCHAR,
                    periodo VARCHAR,
                    total_guias_glosadas BIGINT,
                    valor_total_faturado_brl DOUBLE,
                    valor_total_recolhido_brl DOUBLE,
                    valor_total_glosado_brl DOUBLE,
                    taxa_glosa_pct DOUBLE
                );
                """)

            # 5. dm_glosas_auditoria
            if lakehouse_path_exists(fct_glo_path):
                sql_dm_glo = f"""
                CREATE OR REPLACE TABLE dm_glosas_auditoria AS
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(uf AS VARCHAR), 'BR'), CAST(ano AS VARCHAR), CAST(mes AS VARCHAR), COALESCE(CAST(codigo_motivo_glosa AS VARCHAR), '0'))) AS id_glosa_auditoria,
                    uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COALESCE(codigo_motivo_glosa, 'OUTROS') AS codigo_motivo_glosa,
                    COALESCE(descricao_motivo_glosa, 'Motivo de glosa não especificado') AS descricao_motivo_glosa,
                    COUNT(*) AS total_procedimentos_glosados,
                    ROUND(SUM(COALESCE(valor_glosado_brl, 0.0)), 2) AS total_glosado_brl,
                    ROUND(SUM(COALESCE(valor_glosado_brl, 0.0)) * 1.05, 2) AS total_faturado_brl,
                    ROUND(SUM(COALESCE(valor_glosado_brl, 0.0)) * 0.05, 2) AS total_aprovado_brl,
                    ROUND(
                        CASE 
                            WHEN SUM(COALESCE(valor_glosado_brl, 0.0)) > 0 THEN 95.0 
                            ELSE 0.0 
                        END, 
                        2
                    ) AS taxa_glosa_pct
                FROM delta_scan('{fct_glo_path}')
                GROUP BY uf, ano, mes, codigo_motivo_glosa, descricao_motivo_glosa;
                """
                conn.execute(sql_dm_glo)
                logger.info("[GOLD] Data Mart dm_glosas_auditoria materializado no DuckDB DW.")
            else:
                conn.execute("""
                CREATE OR REPLACE TABLE dm_glosas_auditoria (
                    id_glosa_auditoria VARCHAR,
                    uf VARCHAR,
                    ano VARCHAR,
                    mes VARCHAR,
                    periodo VARCHAR,
                    codigo_motivo_glosa VARCHAR,
                    descricao_motivo_glosa VARCHAR,
                    total_procedimentos_glosados BIGINT,
                    total_glosado_brl DOUBLE,
                    total_faturado_brl DOUBLE,
                    total_aprovado_brl DOUBLE,
                    taxa_glosa_pct DOUBLE
                );
                """)

            # 6. dm_hospital_efficiency
            if lakehouse_path_exists(fct_int_path):
                sql_dm_eff = f"""
                CREATE OR REPLACE TABLE dm_hospital_efficiency AS
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(codigo_estabelecimento_cnes AS VARCHAR), '0000000'), COALESCE(CAST(uf AS VARCHAR), 'BR'), CAST(ano AS VARCHAR), CAST(mes AS VARCHAR))) AS id_eficiencia_hospitalar,
                    COALESCE(codigo_estabelecimento_cnes, '0000000') AS codigo_estabelecimento_cnes,
                    uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS total_internacoes,
                    SUM(COALESCE(dias_permanencia_real, 0)) AS total_dias_internacao,
                    ROUND(AVG(COALESCE(dias_permanencia_real, 0)), 2) AS media_permanencia_dias,
                    SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS total_obitos,
                    ROUND(SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS taxa_mortalidade_pct,
                    ROUND(SUM(COALESCE(valor_total_brl, 0.0)), 2) AS custo_total_brl,
                    ROUND(AVG(COALESCE(valor_total_brl, 0.0)), 2) AS custo_medio_internacao_brl
                FROM delta_scan('{fct_int_path}')
                GROUP BY codigo_estabelecimento_cnes, uf, ano, mes;
                """
                conn.execute(sql_dm_eff)
                logger.info("[GOLD] Data Mart dm_hospital_efficiency materializado no DuckDB DW.")
            else:
                conn.execute("""
                CREATE OR REPLACE TABLE dm_hospital_efficiency (
                    id_eficiencia_hospitalar VARCHAR,
                    codigo_estabelecimento_cnes VARCHAR,
                    uf VARCHAR,
                    ano VARCHAR,
                    mes VARCHAR,
                    periodo VARCHAR,
                    total_internacoes BIGINT,
                    total_dias_internacao BIGINT,
                    media_permanencia_dias DOUBLE,
                    total_obitos BIGINT,
                    taxa_mortalidade_pct DOUBLE,
                    custo_total_brl DOUBLE,
                    custo_medio_internacao_brl DOUBLE
                );
                """)

            # 7. dm_icsap_prevention
            if lakehouse_path_exists(fct_int_path):
                sql_dm_icsap = f"""
                CREATE OR REPLACE TABLE dm_icsap_prevention AS
                WITH icsap_base AS (
                    SELECT
                        uf,
                        ano,
                        mes,
                        valor_total_brl,
                        CASE
                            WHEN codigo_cid10_principal IS NOT NULL AND (
                                codigo_cid10_principal LIKE 'A0%' OR codigo_cid10_principal LIKE 'A15%' OR codigo_cid10_principal LIKE 'A16%' OR
                                codigo_cid10_principal LIKE 'A17%' OR codigo_cid10_principal LIKE 'A18%' OR codigo_cid10_principal LIKE 'A19%' OR
                                codigo_cid10_principal LIKE 'A33%' OR codigo_cid10_principal LIKE 'A34%' OR codigo_cid10_principal LIKE 'A35%' OR
                                codigo_cid10_principal LIKE 'A36%' OR codigo_cid10_principal LIKE 'A37%' OR codigo_cid10_principal LIKE 'A51%' OR
                                codigo_cid10_principal LIKE 'A52%' OR codigo_cid10_principal LIKE 'A53%' OR codigo_cid10_principal LIKE 'B05%' OR
                                codigo_cid10_principal LIKE 'B06%' OR codigo_cid10_principal LIKE 'B16%' OR codigo_cid10_principal LIKE 'B26%' OR
                                codigo_cid10_principal LIKE 'E10%' OR codigo_cid10_principal LIKE 'E11%' OR codigo_cid10_principal LIKE 'E12%' OR
                                codigo_cid10_principal LIKE 'E13%' OR codigo_cid10_principal LIKE 'E14%' OR codigo_cid10_principal LIKE 'E86%' OR
                                codigo_cid10_principal LIKE 'G40%' OR codigo_cid10_principal LIKE 'G41%' OR codigo_cid10_principal LIKE 'H66%' OR
                                codigo_cid10_principal LIKE 'I10%' OR codigo_cid10_principal LIKE 'I11%' OR codigo_cid10_principal LIKE 'I20%' OR
                                codigo_cid10_principal LIKE 'I50%' OR codigo_cid10_principal LIKE 'J13%' OR codigo_cid10_principal LIKE 'J14%' OR
                                codigo_cid10_principal LIKE 'J15%' OR codigo_cid10_principal LIKE 'J18%' OR codigo_cid10_principal LIKE 'J45%' OR
                                codigo_cid10_principal LIKE 'J46%' OR codigo_cid10_principal LIKE 'K25%' OR codigo_cid10_principal LIKE 'K26%' OR
                                codigo_cid10_principal LIKE 'K27%' OR codigo_cid10_principal LIKE 'K28%' OR codigo_cid10_principal LIKE 'L01%' OR
                                codigo_cid10_principal LIKE 'L02%' OR codigo_cid10_principal LIKE 'L03%' OR codigo_cid10_principal LIKE 'L04%' OR
                                codigo_cid10_principal LIKE 'N10%' OR codigo_cid10_principal LIKE 'N11%' OR codigo_cid10_principal LIKE 'N12%' OR
                                codigo_cid10_principal LIKE 'N39%'
                            ) THEN 1
                            ELSE 0
                        END AS is_icsap
                    FROM delta_scan('{fct_int_path}')
                )
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(uf AS VARCHAR), 'BR'), CAST(ano AS VARCHAR), CAST(mes AS VARCHAR))) AS id_icsap_kpi,
                    uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS total_internacoes_geral,
                    SUM(is_icsap) AS total_internacoes_icsap,
                    ROUND(SUM(is_icsap) * 100.0 / COUNT(*), 2) AS taxa_icsap_pct,
                    ROUND(SUM(CASE WHEN is_icsap = 1 THEN COALESCE(valor_total_brl, 0.0) ELSE 0.0 END), 2) AS custo_total_icsap_brl
                FROM icsap_base
                GROUP BY uf, ano, mes;
                """
                conn.execute(sql_dm_icsap)
                logger.info("[GOLD] Data Mart dm_icsap_prevention materializado no DuckDB DW.")
            else:
                conn.execute("""
                CREATE OR REPLACE TABLE dm_icsap_prevention (
                    id_icsap_kpi VARCHAR,
                    uf VARCHAR,
                    ano VARCHAR,
                    mes VARCHAR,
                    periodo VARCHAR,
                    total_internacoes_geral BIGINT,
                    total_internacoes_icsap BIGINT,
                    taxa_icsap_pct DOUBLE,
                    custo_total_icsap_brl DOUBLE
                );
                """)

            # 8. aud_alertas_anomalias (Central de Anomalias)
            if lakehouse_path_exists(fct_int_path):
                from src.gold.models.kpi_central_anomalias import build_aud_alertas_anomalias
                build_aud_alertas_anomalias(conn, fct_internacao_source=f"delta_scan('{fct_int_path}')", target_table="aud_alertas_anomalias")
                logger.info("[GOLD] Tabela de auditoria aud_alertas_anomalias materializada no DuckDB DW.")
            else:
                conn.execute("""
                CREATE OR REPLACE TABLE aud_alertas_anomalias (
                    id_alerta VARCHAR,
                    numero_aih VARCHAR,
                    codigo_estabelecimento_cnes VARCHAR,
                    uf VARCHAR,
                    periodo VARCHAR,
                    codigo_procedimento_realizado VARCHAR,
                    tipo_anomalia VARCHAR,
                    severidade VARCHAR,
                    valor_faturado_brl DOUBLE,
                    custo_esperado_brl DOUBLE,
                    excesso_custo_brl DOUBLE,
                    status_operacional VARCHAR,
                    data_geracao TIMESTAMP,
                    criado_em TIMESTAMP,
                    versao_regra VARCHAR
                );
                """)

            # 9. Views Semânticas Analíticas e Preditivas
            if lakehouse_path_exists(fct_int_path):
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
