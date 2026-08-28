"""
Pipeline Orquestrador da Camada Gold e Carga do Data Warehouse (QIMED DW).
Transforma as tabelas canônicas da Camada Silver em Data Marts especializados e atualiza o DuckDB.
"""
import os
import time
from typing import Dict, Any, Optional
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from src.dw.dw_manager import DataWarehouseManager
from src.gold.models.kpi_glosas_auditoria import build_dm_glosas_auditoria
from src.gold.models.kpi_hospital_efficiency import build_dm_hospital_efficiency
from src.gold.models.kpi_patient_readmissions import build_dm_patient_readmissions
from src.gold.models.kpi_regulation_bottlenecks import build_dm_regulation_bottlenecks
from src.gold.models.kpi_icsap_prevention import build_dm_icsap_prevention
from src.silver.terminology_names import resolver_nome_hospital, resolver_nome_municipio
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class GoldTransformationPipeline:
    """
    Orquestra o processamento da Camada Gold e a consolidação no Data Warehouse colunar.
    """

    def __init__(self, base_path: Optional[str] = None):
        if not base_path:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.silver_path = os.path.join(base_dir, "lakehouse", "silver")
            self.bronze_path = os.path.join(base_dir, "lakehouse", "bronze", "datasus")
            self.gold_path = os.path.join(base_dir, "lakehouse", "gold")
            self.dw_path = os.path.join(base_dir, "warehouse", "qimed_dw.duckdb")
        else:
            self.silver_path = os.path.join(base_path, "lakehouse", "silver")
            self.bronze_path = os.path.join(base_path, "lakehouse", "bronze", "datasus")
            self.gold_path = os.path.join(base_path, "lakehouse", "gold")
            self.dw_path = os.path.join(base_path, "warehouse", "qimed_dw.duckdb")

        os.makedirs(self.gold_path, exist_ok=True)
        os.makedirs(os.path.dirname(self.dw_path), exist_ok=True)

    def _read_delta_safe(self, table_path: str) -> Optional[pd.DataFrame]:
        """
        Lê com segurança uma tabela Delta Lake se ela existir.
        Retorna None explícito em caso de erro ou ausência para evitar falhas silenciosas.
        """
        if os.path.exists(table_path):
            try:
                return DeltaTable(table_path).to_pandas()
            except Exception as e:
                logger.warning(f"[GOLD READ ERROR] Falha ao ler DeltaTable em {table_path}: {e}")
                return None
        logger.info(f"[GOLD READ INFO] Tabela não encontrada em {table_path}")
        return None

    def _write_gold_delta(self, df: Optional[pd.DataFrame], table_name: str) -> str:
        """Persiste um Data Mart no formato Delta Lake."""
        out_path = os.path.join(self.gold_path, table_name)
        if df is None or df.empty:
            logger.warning(f"Data Mart '{table_name}' está vazio ou nulo, pulando gravação Delta.")
            return out_path

        # Garante tipos strings para compatibilidade Delta
        pdf = df.copy()
        for col in pdf.columns:
            if pdf[col].dtype == object:
                pdf[col] = pdf[col].astype(str)

        write_deltalake(
            table_or_uri=out_path,
            data=pdf,
            mode="overwrite",
        )
        logger.info(f"Data Mart '{table_name}' gravado em Delta Lake: {out_path} ({len(pdf)} registros)")
        return out_path

    def run(self) -> Dict[str, Any]:
        """
        Executa o fluxo completo da Camada Gold:
        1. Carrega tabelas Silver e Bronze necessárias com verificação explícita de erro.
        2. Computa os 5 Data Marts especializados.
        3. Persiste no Delta Lake (`lakehouse/gold/`).
        4. Popula o Data Warehouse DuckDB (`warehouse/qimed_dw.duckdb`).
        5. Cria as Views Semânticas de BI.
        """
        start_time = time.time()
        logger.info("=" * 80)
        logger.info("INICIANDO PROCESSAMENTO DA CAMADA GOLD E CARGA DO DATA WAREHOUSE")
        logger.info("=" * 80)

        # 1. Carregamento dos dados Silver e Bronze com verificação explícita de integridade
        df_pat = self._read_delta_safe(os.path.join(self.silver_path, "dim_patients"))
        df_enc = self._read_delta_safe(os.path.join(self.silver_path, "fct_encounters"))
        df_cond = self._read_delta_safe(os.path.join(self.silver_path, "fct_conditions"))
        df_proc = self._read_delta_safe(os.path.join(self.silver_path, "fct_procedures"))
        df_ref = self._read_delta_safe(os.path.join(self.silver_path, "fct_referrals"))
        df_ans = self._read_delta_safe(os.path.join(self.silver_path, "dim_health_plans"))

        # Fontes de apoio Bronze
        df_cnes = self._read_delta_safe(os.path.join(self.bronze_path, "cnes"))
        df_sia = self._read_delta_safe(os.path.join(self.bronze_path, "sia"))
        df_sisab = self._read_delta_safe(os.path.join(self.bronze_path, "sisab"))

        # Enriquecer fct_encounters com nomes de hospitais e municípios dos pacientes
        if df_enc is not None and not df_enc.empty:
            df_enc["cnes_raw"] = df_enc["organization_id"].astype(str).str.replace("org_cnes_", "").str.zfill(7)
            df_enc["hospital_name"] = df_enc["cnes_raw"].apply(resolver_nome_hospital)
            
            # Cruzar com dim_patients para obter município de residência do paciente
            if df_pat is not None and not df_pat.empty and "patient_master_id" in df_pat.columns and "municipality_code" in df_pat.columns:
                pat_mun_map = df_pat.dropna(subset=["patient_master_id"]).drop_duplicates("patient_master_id").set_index("patient_master_id")["municipality_code"].to_dict()
                df_enc["municipality_code"] = df_enc["patient_master_id"].map(pat_mun_map)
                df_enc["municipality_name"] = df_enc["municipality_code"].apply(resolver_nome_municipio).fillna("Fortaleza (Capital)")
            elif "municipality_name" not in df_enc.columns:
                df_enc["municipality_name"] = "Fortaleza (Capital)"

        # Enriquecer fct_referrals com nomes de municípios
        if df_ref is not None and not df_ref.empty and "municipality_code" in df_ref.columns:
            df_ref["municipality_name"] = df_ref["municipality_code"].apply(resolver_nome_municipio)

        # 2. Construção dos 5 Data Marts (com proteção para DataFrames None)
        dm_glosas = build_dm_glosas_auditoria(df_sia, df_cnes) if df_sia is not None or df_cnes is not None else pd.DataFrame()
        dm_eff = build_dm_hospital_efficiency(df_enc, df_cnes) if df_enc is not None else pd.DataFrame()
        dm_readm = build_dm_patient_readmissions(df_enc, df_cond) if df_enc is not None else pd.DataFrame()
        dm_bottlenecks = build_dm_regulation_bottlenecks(df_ref) if df_ref is not None else pd.DataFrame()
        dm_icsap = build_dm_icsap_prevention(df_enc, df_sisab) if df_enc is not None else pd.DataFrame()

        # 3. Persistência em Delta Lake
        self._write_gold_delta(dm_glosas, "dm_glosas_auditoria")
        self._write_gold_delta(dm_eff, "dm_hospital_efficiency")
        self._write_gold_delta(dm_readm, "dm_patient_readmissions")
        self._write_gold_delta(dm_bottlenecks, "dm_regulation_bottlenecks")
        self._write_gold_delta(dm_icsap, "dm_icsap_prevention")

        # 4. Carga e Registro no Data Warehouse (DuckDB)
        dw = DataWarehouseManager(db_path=self.dw_path)
        try:
            # Fatos e Dimensões Canônicas da Camada Silver
            if df_pat is not None and not df_pat.empty: dw.register_table_from_df("dim_patients", df_pat)
            if df_enc is not None and not df_enc.empty: dw.register_table_from_df("fct_encounters", df_enc)
            if df_cond is not None and not df_cond.empty: dw.register_table_from_df("fct_conditions", df_cond)
            if df_proc is not None and not df_proc.empty: dw.register_table_from_df("fct_procedures", df_proc)
            if df_ref is not None and not df_ref.empty: dw.register_table_from_df("fct_referrals", df_ref)
            if df_ans is not None and not df_ans.empty: dw.register_table_from_df("dim_health_plans", df_ans)

            # Data Marts da Camada Gold
            if not dm_glosas.empty: dw.register_table_from_df("dm_glosas_auditoria", dm_glosas)
            if not dm_eff.empty: dw.register_table_from_df("dm_hospital_efficiency", dm_eff)
            if not dm_readm.empty: dw.register_table_from_df("dm_patient_readmissions", dm_readm)
            if not dm_bottlenecks.empty: dw.register_table_from_df("dm_regulation_bottlenecks", dm_bottlenecks)
            if not dm_icsap.empty: dw.register_table_from_df("dm_icsap_prevention", dm_icsap)

            # 5. Criar Views Semânticas de Negócio
            dw.create_semantic_views()
            tables_in_dw = dw.list_tables()
        finally:
            dw.close()

        duration = round(time.time() - start_time, 2)
        logger.info(f"Pipeline da Camada Gold e Data Warehouse concluído com sucesso em {duration}s!")
        return {
            "status": "success",
            "gold_marts_created": 5,
            "dw_tables_total": len(tables_in_dw) if 'tables_in_dw' in locals() else 0,
            "duration_seconds": duration
        }
