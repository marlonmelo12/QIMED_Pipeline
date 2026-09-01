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
    Consolida as tabelas agregadas Gold (Data Marts) 100% em português diretamente no DuckDB DW
    utilizando a estratégia atômica de Build-Then-Swap.
    """

    def __init__(self, dw_file_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        base_warehouse = dw_file_path or self.cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")
        os.makedirs(os.path.dirname(base_warehouse), exist_ok=True)
        self.dw_file_path = base_warehouse
        self.silver_dir = self.cfg.get("paths", {}).get("silver_dir", "lakehouse/silver")
        self.lineage_tracker = DataLineageTracker()

    def _validate_gold_dw_integrity(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Executa validações de qualidade e integridade no DW em construção antes de autorizar o swap.
        Garante que tabelas críticas existam e não estejam corrompidas.
        """
        tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
        required_tables = ["agg_internacoes_uf", "agg_perfil_epidemiologico", "dm_hospital_efficiency", "aud_alertas_anomalias"]
        for t in required_tables:
            if t not in tables:
                raise ValueError(f"[DATA QUALITY FAIL] Tabela obrigatória ausente no Gold DW: {t}")
        
        # Validação de contagem
        count_int = conn.execute("SELECT COUNT(*) FROM agg_internacoes_uf;").fetchone()[0]
        from src.utils.s3_storage import lakehouse_path_exists
        fct_int_path = os.path.join(self.silver_dir, "fct_internacao").replace("\\", "/")
        if lakehouse_path_exists(fct_int_path) and count_int == 0:
            raise ValueError("[DATA QUALITY FAIL] agg_internacoes_uf possui 0 registros mesmo com Silver disponível.")
            
        logger.info(f"[DATA QUALITY PASS] Validação de integridade do Gold DW aprovada ({len(tables)} tabelas/views).")

    def build_gold_data_marts(self, execution_id: str = "exec_gold") -> Dict[str, Any]:
        """
        Gera e materializa todos os Data Marts analíticos utilizando o padrão
        Build-Then-Swap atômico:
        1. Materializa o novo DW em um arquivo temporário isolado (warehouse/qimed_dw_building_<id>.duckdb).
        2. Valida a integridade das tabelas e métricas principais.
        3. Realiza a substituição atômica via os.replace() para o arquivo de produção.
        4. Invalida os caches dos workers da API.
        """
        import uuid
        import time
        t0 = time.time()
        uid = uuid.uuid4().hex[:6]
        building_dw_file = os.path.join(
            os.path.dirname(self.dw_file_path),
            f"qimed_dw_building_{execution_id}_{int(time.time())}_{uid}.duckdb"
        )
        os.makedirs(os.path.dirname(building_dw_file), exist_ok=True)

        logger.info(f"[GOLD BUILD] Iniciando materialização em arquivo isolado: {building_dw_file}")
        conn = duckdb.connect(building_dw_file)
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

            # 4. dm_ans_glosas_operadoras
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
                GROUP BY r.codigo_registro_ans, o.codigo_registro_ans, o.cnpj_operadora, o.razao_social, r.razao_social_operadora, r.modalidade_operadora, o.modalidade_operadora, r.ano, r.mes;
                """
                conn.execute(sql_dm_ans)
                logger.info("[GOLD] Data Mart dm_ans_glosas_operadoras materializado no DuckDB DW.")

            # 5. dm_hospital_efficiency
            if lakehouse_path_exists(fct_int_path):
                sql_dm_eff = f"""
                CREATE OR REPLACE TABLE dm_hospital_efficiency AS
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(codigo_estabelecimento_cnes AS VARCHAR), '0000000'), CAST(ano AS VARCHAR), CAST(mes AS VARCHAR))) AS id_eficiencia_hospitalar,
                    COALESCE(codigo_estabelecimento_cnes, '0000000') AS codigo_estabelecimento_cnes,
                    COALESCE(uf, 'BR') AS uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS total_internacoes,
                    SUM(COALESCE(dias_permanencia_real, 0)) AS total_dias_permanencia,
                    ROUND(AVG(COALESCE(dias_permanencia_real, 0)), 2) AS media_permanencia_dias,
                    SUM(CASE WHEN indicador_obito = TRUE THEN 1 ELSE 0 END) AS total_obitos,
                    ROUND(SUM(CASE WHEN indicador_obito = TRUE THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) AS taxa_mortalidade_pct,
                    ROUND(SUM(COALESCE(valor_total_brl, 0.0)), 2) AS custo_total_brl,
                    ROUND(AVG(COALESCE(valor_total_brl, 0.0)), 2) AS ticket_medio_brl
                FROM delta_scan('{fct_int_path}')
                GROUP BY codigo_estabelecimento_cnes, uf, ano, mes;
                """
                conn.execute(sql_dm_eff)
                logger.info("[GOLD] Data Mart dm_hospital_efficiency materializado no DuckDB DW.")

            # 6. dm_glosas_auditoria
            if lakehouse_path_exists(fct_glo_path):
                sql_dm_glo = f"""
                CREATE OR REPLACE TABLE dm_glosas_auditoria AS
                SELECT
                    md5(concat_ws('-', COALESCE(CAST(codigo_estabelecimento_cnes AS VARCHAR), '0000000'), COALESCE(CAST(codigo_motivo_glosa AS VARCHAR), '00'), CAST(ano AS VARCHAR), CAST(mes AS VARCHAR))) AS id_glosa_auditoria,
                    COALESCE(codigo_estabelecimento_cnes, '0000000') AS codigo_estabelecimento_cnes,
                    COALESCE(codigo_motivo_glosa, '99') AS codigo_motivo_glosa,
                    COALESCE(descricao_motivo_glosa, 'GLOSA ADMINISTRATIVA / TÉCNICA') AS descricao_motivo_glosa,
                    COALESCE(tipo_glosa, 'ADMINISTRATIVA') AS tipo_glosa,
                    COALESCE(uf, 'BR') AS uf,
                    CAST(ano AS VARCHAR) AS ano,
                    LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                    CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                    COUNT(*) AS quantidade_itens_glosados,
                    ROUND(SUM(COALESCE(valor_glosado_brl, 0.0)), 2) AS valor_total_glosado_brl
                FROM delta_scan('{fct_glo_path}')
                GROUP BY codigo_estabelecimento_cnes, codigo_motivo_glosa, descricao_motivo_glosa, tipo_glosa, uf, ano, mes;
                """
                conn.execute(sql_dm_glo)
                logger.info("[GOLD] Data Mart dm_glosas_auditoria materializado no DuckDB DW.")

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

            # 8. aud_alertas_anomalias (Central de Anomalias)
            if lakehouse_path_exists(fct_int_path):
                from src.gold.models.kpi_central_anomalias import build_aud_alertas_anomalias
                build_aud_alertas_anomalias(conn, fct_internacao_source=f"delta_scan('{fct_int_path}')", target_table="aud_alertas_anomalias")
                logger.info("[GOLD] Tabela de auditoria aud_alertas_anomalias materializada no DuckDB DW.")

            # 9. NOVO: dm_kpi_dashboard_financeiro (Pre-aggregation O(1))
            if lakehouse_path_exists(fct_int_path):
                sql_kpi_dash = f"""
                CREATE OR REPLACE TABLE dm_kpi_dashboard_financeiro AS
                WITH base_int AS (
                    SELECT
                        CAST(ano AS VARCHAR) AS ano,
                        LPAD(CAST(mes AS VARCHAR), 2, '0') AS mes,
                        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                        uf,
                        valor_total_brl,
                        indicador_obito,
                        dias_permanencia_real
                    FROM delta_scan('{fct_int_path}')
                ),
                kpi_nacional AS (
                    SELECT
                        periodo,
                        '' AS uf,
                        COUNT(*) AS total_internacoes,
                        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
                        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
                        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END) / NULLIF(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0), 0.0), 2) AS razao_custo_obito_alta,
                        0.0 AS taxa_glosa_pct
                    FROM base_int
                    GROUP BY periodo
                ),
                kpi_uf AS (
                    SELECT
                        periodo,
                        uf,
                        COUNT(*) AS total_internacoes,
                        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
                        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
                        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END), 0.0), 2) AS custo_medio_obito_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0.0), 2) AS custo_medio_alta_brl,
                        ROUND(COALESCE(AVG(CASE WHEN indicador_obito = TRUE THEN valor_total_brl END) / NULLIF(AVG(CASE WHEN indicador_obito = FALSE THEN valor_total_brl END), 0), 0.0), 2) AS razao_custo_obito_alta,
                        0.0 AS taxa_glosa_pct
                    FROM base_int
                    GROUP BY periodo, uf
                )
                SELECT * FROM kpi_nacional
                UNION ALL
                SELECT * FROM kpi_uf;
                """
                conn.execute(sql_kpi_dash)
                logger.info("[GOLD] Data Mart dm_kpi_dashboard_financeiro pré-agregado com sucesso.")

            # 10. NOVO: dm_kpi_permanencia_faixa (Pre-aggregation O(1))
            if lakehouse_path_exists(fct_int_path):
                sql_faixa = f"""
                CREATE OR REPLACE TABLE dm_kpi_permanencia_faixa AS
                WITH base AS (
                    SELECT
                        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                        uf,
                        CASE
                            WHEN dias_permanencia_real <= 3 THEN '0-3d'
                            WHEN dias_permanencia_real <= 7 THEN '4-7d'
                            WHEN dias_permanencia_real <= 14 THEN '8-14d'
                            ELSE '15d+'
                        END AS faixa_permanencia,
                        valor_total_brl,
                        dias_permanencia_real
                    FROM delta_scan('{fct_int_path}')
                ),
                faixas_nacional AS (
                    SELECT
                        periodo,
                        '' AS uf,
                        faixa_permanencia,
                        COUNT(*) AS total_internacoes,
                        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                        ROUND(COALESCE(AVG(dias_permanencia_real), 0.0), 1) AS dias_medios
                    FROM base
                    GROUP BY periodo, faixa_permanencia
                ),
                faixas_uf AS (
                    SELECT
                        periodo,
                        uf,
                        faixa_permanencia,
                        COUNT(*) AS total_internacoes,
                        ROUND(COALESCE(SUM(valor_total_brl), 0.0), 2) AS custo_total_brl,
                        ROUND(COALESCE(AVG(dias_permanencia_real), 0.0), 1) AS dias_medios
                    FROM base
                    GROUP BY periodo, uf, faixa_permanencia
                )
                SELECT * FROM faixas_nacional
                UNION ALL
                SELECT * FROM faixas_uf;
                """
                conn.execute(sql_faixa)
                logger.info("[GOLD] Data Mart dm_kpi_permanencia_faixa pré-agregado com sucesso.")

            # 11. NOVO: dm_kpi_percentis_hospitalares (Pre-aggregation O(1))
            if lakehouse_path_exists(fct_int_path):
                sql_perc = f"""
                CREATE OR REPLACE TABLE dm_kpi_percentis_hospitalares AS
                WITH base AS (
                    SELECT
                        CAST(ano AS VARCHAR) || '-' || LPAD(CAST(mes AS VARCHAR), 2, '0') AS periodo,
                        uf,
                        valor_total_brl
                    FROM delta_scan('{fct_int_path}')
                ),
                perc_nacional AS (
                    SELECT
                        periodo,
                        '' AS uf,
                        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
                        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
                        ROUND(COALESCE(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p25,
                        ROUND(COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p50,
                        ROUND(COALESCE(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p75,
                        ROUND(COALESCE(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p90,
                        ROUND(COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p99,
                        ROUND(COALESCE(MAX(valor_total_brl), 0.0), 2) AS maximo_custo_brl,
                        ROUND(COALESCE(MIN(valor_total_brl), 0.0), 2) AS minimo_custo_brl,
                        ROUND(COALESCE(STDDEV_SAMP(valor_total_brl), 0.0), 2) AS desvio_padrao_brl,
                        COUNT(*) AS total_internacoes
                    FROM base
                    GROUP BY periodo
                ),
                perc_uf AS (
                    SELECT
                        periodo,
                        uf,
                        ROUND(COALESCE(AVG(valor_total_brl), 0.0), 2) AS ticket_medio_brl,
                        ROUND(COALESCE(MEDIAN(valor_total_brl), 0.0), 2) AS mediana_custo_brl,
                        ROUND(COALESCE(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p25,
                        ROUND(COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p50,
                        ROUND(COALESCE(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p75,
                        ROUND(COALESCE(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p90,
                        ROUND(COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY valor_total_brl), 0.0), 2) AS p99,
                        ROUND(COALESCE(MAX(valor_total_brl), 0.0), 2) AS maximo_custo_brl,
                        ROUND(COALESCE(MIN(valor_total_brl), 0.0), 2) AS minimo_custo_brl,
                        ROUND(COALESCE(STDDEV_SAMP(valor_total_brl), 0.0), 2) AS desvio_padrao_brl,
                        COUNT(*) AS total_internacoes
                    FROM base
                    GROUP BY periodo, uf
                )
                SELECT * FROM perc_nacional
                UNION ALL
                SELECT * FROM perc_uf;
                """
                conn.execute(sql_perc)
                logger.info("[GOLD] Data Mart dm_kpi_percentis_hospitalares pré-agregado com sucesso.")

            # 12. Materialização física de fct_internacao e Views Semânticas
            if lakehouse_path_exists(fct_int_path):
                from src.gold.models.views_semanticas import registrar_views_semanticas
                try:
                    conn.execute(f"CREATE OR REPLACE TABLE fct_internacao AS SELECT * FROM delta_scan('{fct_int_path}')")
                except Exception as e:
                    logger.warning(f"Fallback para view fct_internacao: {e}")
                    conn.execute(f"CREATE OR REPLACE VIEW fct_internacao AS SELECT * FROM delta_scan('{fct_int_path}')")
                registrar_views_semanticas(conn)

            # Executa validação de qualidade estrita pré-swap
            self._validate_gold_dw_integrity(conn)

            # Lista tabelas finais criadas
            tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]
            
            # Fecha a conexão DuckDB antes do atomic swap de arquivo
            conn.close()
            conn = None

            # Substituição atômica via os.replace() (garante 0 erros de lock na API)
            os.replace(building_dw_file, self.dw_file_path)
            dur = round(time.time() - t0, 2)
            logger.info(f"[GOLD ATOMIC SWAP] DW atualizado com sucesso via swap atômico para {self.dw_file_path} em {dur}s!")

            # Invalida o cache distribuído da API
            try:
                from src.api.cache import invalidate_all_cache
                invalidate_all_cache()
            except Exception as e:
                logger.warning(f"Aviso ao invalidar cache da API após swap atômico: {e}")

            return {"status": "success", "tables_created": tables, "dw_file": self.dw_file_path, "duration_seconds": dur}
        except Exception as e:
            logger.error(f"[GOLD ATOMIC BUILD FAILED] Erro na materialização do DW: {e}", exc_info=True)
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if os.path.exists(building_dw_file):
                try:
                    os.remove(building_dw_file)
                except Exception:
                    pass
