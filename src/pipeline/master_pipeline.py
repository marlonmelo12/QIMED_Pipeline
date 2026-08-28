"""
QIMED Master Pipeline Orchestrator V3 - Lakehouse Health Platform.
Orquestra o fluxo ponta a ponta com baixo consumo de RAM, garantia de idempotencia,
controle de concorrencia (PartitionLockManager), recuperacao (Recovery),
governanca de dados (Manifest & Lineage Delta), DuckDB out-of-core e fail-fast estrito.
"""
import os
import time
import uuid
import json
import traceback
from typing import Any, Dict, List, Optional
import psutil

from src.collectors.datasus_collector import DatasusCollector
from src.ingestion.staging_writer import ParquetStagingWriter
from src.ingestion.batch_sink import DeltaBatchSink
from src.ingestion.lock_manager import PartitionLockManager
from src.ingestion.manifest_delta import DeltaManifestManager
from src.processing.duckdb_engine import DuckDBEngine
from src.processing.transformations import CanonicalTransformations
from src.gold.pipeline_nacional import GoldPipelineNacional
from src.quality.data_quality import DataQualityAuditor
from src.observability.metrics import MetricsCollector
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)

UF_LIST = [
    "RO", "AC", "AM", "RR", "PA", "AP", "TO",
    "MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA",
    "MG", "ES", "RJ", "SP", "PR", "SC", "RS", "MS", "MT", "GO", "DF"
]


class QimedMasterPipeline:
    """
    Orquestrador Mestre V3 com controle de concorrencia, recovery e rastreabilidade total.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.cfg = load_pipeline_config(config_path)
        self.metrics_collector = MetricsCollector()
        self.lock_manager = PartitionLockManager()
        self.manifest_manager = DeltaManifestManager()
        self.process = psutil.Process(os.getpid())

    def _update_progress(self, etapa: str, detalhe: str, pct: float, extra: Optional[Dict[str, Any]] = None):
        """
        Atualiza a telemetria em tempo real no arquivo pipeline_progress.json.
        """
        p_path = os.path.join(self.cfg["paths"]["lakehouse_root"], "pipeline_progress.json")
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "etapa": etapa,
            "detalhe": detalhe,
            "progresso_pct": pct,
            "memoria_ram_mb": round(self.process.memory_info().rss / (1024 * 1024), 1),
            "extra": extra or {},
        }
        try:
            with open(p_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.debug(f"Falha ao atualizar telemetria de progresso ({p_path}): {e}")

    def execute_bronze_ingestion(
        self,
        target_month: int = 5,
        target_year: int = 2026,
        force_reprocess: bool = False,
        execution_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executa a ingestão Bronze (SIH e SIA) para todas as 27 UFs com controle transacional Delta.
        """
        exec_id = execution_id or f"exec_bronze_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        total_ufs = len(UF_LIST)
        total_sih_rows = 0
        total_sia_rows = 0
        failed_partitions: List[Dict[str, str]] = []
        peak_rss = 0.0

        # SIH
        t0_sih = time.time()
        logger.info(f"Ingestao Bronze SIH ({target_month:02d}/{target_year})...")
        for idx, uf in enumerate(UF_LIST, 1):
            pct = round((idx / total_ufs) * 50.0, 1)
            self._update_progress("Bronze - Ingestao SIH", f"[{idx}/{total_ufs}] Processando SIH-{uf}...", pct)
            partition_key = f"SIH/{target_year}/{target_month:02d}/{uf}"

            if not force_reprocess and self.manifest_manager.is_partition_committed("SIH", target_year, target_month, uf):
                logger.info(f"[RECOVERY - SKIP] Particao {partition_key} ja commitada com sucesso no Delta Lake.")
                continue

            try:
                with self.lock_manager.lock(partition_key, exec_id):
                    col = DatasusCollector(subsystem="SIH", uf=uf, year=target_year, month=target_month)
                    raw_file = col.fetch()
                    stg = ParquetStagingWriter(subsystem="sih", year=target_year, month=target_month, uf=uf)
                    stg.process_batch_stream(col.parse_record_batches(raw_file, chunksize=100000))

                    sink = DeltaBatchSink(subsystem="sih", year=target_year, month=target_month, uf=uf)
                    commit_res = sink.commit_staging_to_bronze(stg, execution_id=exec_id, source_files=[raw_file])
                    total_sih_rows += commit_res.get("total_rows", 0)
            except Exception as e:
                err_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(f"[ERRO CRITICO] Falha ao processar {partition_key}: {err_msg}\n{traceback.format_exc()}")
                self.manifest_manager.record_manifest_entry(
                    subsystem="SIH",
                    year=target_year,
                    month=target_month,
                    uf=uf,
                    files=["error"],
                    total_rows=0,
                    status="failed",
                    execution_id=exec_id,
                    error_message=err_msg,
                )
                failed_partitions.append({"partition": partition_key, "error": err_msg})

            current_rss = self.process.memory_info().rss / (1024 * 1024)
            peak_rss = max(peak_rss, current_rss)

        dur_sih = time.time() - t0_sih
        self.metrics_collector.record_stage_metrics(exec_id, "Bronze SIH", dur_sih, total_sih_rows, peak_rss)

        # SIA
        t0_sia = time.time()
        logger.info(f"Ingestao Bronze SIA com Auto-Discovery Multipart ({target_month:02d}/{target_year})...")
        for idx, uf in enumerate(UF_LIST, 1):
            pct = round(50.0 + (idx / total_ufs) * 50.0, 1)
            self._update_progress("Bronze - Ingestao SIA", f"[{idx}/{total_ufs}] Processando SIA-{uf}...", pct)
            partition_key = f"SIA/{target_year}/{target_month:02d}/{uf}"

            if not force_reprocess and self.manifest_manager.is_partition_committed("SIA", target_year, target_month, uf):
                logger.info(f"[RECOVERY - SKIP] Particao {partition_key} ja commitada com sucesso no Delta Lake.")
                continue

            try:
                with self.lock_manager.lock(partition_key, exec_id):
                    col = DatasusCollector(subsystem="SIA", uf=uf, year=target_year, month=target_month)
                    raw_files = col.fetch()
                    file_list = raw_files if isinstance(raw_files, list) else [raw_files]

                    stg = ParquetStagingWriter(subsystem="sia", year=target_year, month=target_month, uf=uf)
                    for f in file_list:
                        stg.process_batch_stream(col.parse_record_batches(f, chunksize=100000))

                    sink = DeltaBatchSink(subsystem="sia", year=target_year, month=target_month, uf=uf)
                    commit_res = sink.commit_staging_to_bronze(stg, execution_id=exec_id, source_files=file_list)
                    total_sia_rows += commit_res.get("total_rows", 0)
            except Exception as e:
                err_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(f"[ERRO CRITICO] Falha ao processar {partition_key}: {err_msg}\n{traceback.format_exc()}")
                self.manifest_manager.record_manifest_entry(
                    subsystem="SIA",
                    year=target_year,
                    month=target_month,
                    uf=uf,
                    files=["error"],
                    total_rows=0,
                    status="failed",
                    execution_id=exec_id,
                    error_message=err_msg,
                )
                failed_partitions.append({"partition": partition_key, "error": err_msg})

            current_rss = self.process.memory_info().rss / (1024 * 1024)
            peak_rss = max(peak_rss, current_rss)

        dur_sia = time.time() - t0_sia
        self.metrics_collector.record_stage_metrics(exec_id, "Bronze SIA", dur_sia, total_sia_rows, peak_rss)

        return {
            "id_execucao": exec_id,
            "total_sih_rows": total_sih_rows,
            "total_sia_rows": total_sia_rows,
            "failed_partitions": failed_partitions,
            "peak_rss_mb": round(peak_rss, 2),
        }

    def execute_silver_transformation(
        self,
        target_month: int = 5,
        target_year: int = 2026,
        execution_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executa as transformações canônicas Silver no DuckDB (Star Schema, MPI, Normalização).
        """
        exec_id = execution_id or f"exec_silver_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        t0 = time.time()
        self._update_progress("Silver - Transformacoes", "Executando Star Schema canonico no DuckDB...", 75.0)
        logger.info("Executando transformacoes da Camada Silver...")

        duck_engine = DuckDBEngine(config=self.cfg)
        try:
            silver_transforms = CanonicalTransformations(duck_engine=duck_engine, config=self.cfg)
            silver_transforms.gerar_dim_tempo(start_year=target_year-1, end_year=target_year+1, execution_id=exec_id)
            silver_transforms.transformar_ans_para_silver(execution_id=exec_id)
            silver_transforms.transformar_sih_para_silver(execution_id=exec_id)
            silver_transforms.transformar_sia_para_silver(execution_id=exec_id)
            silver_transforms.transformar_glosas_hospitalares_para_silver(execution_id=exec_id)

            dur = time.time() - t0
            self.metrics_collector.record_stage_metrics(exec_id, "Silver Transforms", dur, 0, 0)
            return {"status": "success", "duration_seconds": round(dur, 2), "id_execucao": exec_id}
        finally:
            duck_engine.close()

    def execute_gold_aggregation(
        self,
        target_month: int = 5,
        target_year: int = 2026,
        execution_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Consolida os Data Marts Gold e views semânticas no DuckDB DW.
        """
        exec_id = execution_id or f"exec_gold_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        t0 = time.time()
        self._update_progress("Gold - Data Marts", "Materializando Data Marts no DuckDB DW...", 90.0)
        logger.info("Materializando Data Marts Gold no DuckDB DW...")

        gold_pipeline = GoldPipelineNacional(config=self.cfg)
        gold_res = gold_pipeline.build_gold_data_marts(execution_id=exec_id)
        dur = time.time() - t0
        self.metrics_collector.record_stage_metrics(exec_id, "Gold DW", dur, 0, 0)
        return {"status": "success", "duration_seconds": round(dur, 2), "data_marts": gold_res, "id_execucao": exec_id}

    def execute_data_quality_audit(self, execution_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Executa a auditoria forense de integridade, conformidade e consistência de dados.
        """
        exec_id = execution_id or f"exec_audit_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        self._update_progress("Qualidade & Auditoria", "Auditando integridade e consistencia de dados...", 98.0)
        logger.info("Executando Auditoria Forense de Data Quality...")

        auditor = DataQualityAuditor(config=self.cfg)
        quality_res = auditor.audit_full_warehouse()
        return {"status": "success", "id_execucao": exec_id, "quality_audit": quality_res}

    def run_full_pipeline(
        self,
        target_month: int = 5,
        target_year: int = 2026,
        force_reprocess: bool = False
    ) -> Dict[str, Any]:
        """
        Executa todas as etapas do Lakehouse V3 de ponta a ponta.
        """
        execution_id = f"exec_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        t0_total = time.time()

        logger.info("=" * 88)
        logger.info(f"INICIANDO EXECUCAO MASTER PIPELINE QIMED LAKEHOUSE V3 (ID: {execution_id})")
        logger.info("=" * 88)

        # 1. Bronze
        bronze_res = self.execute_bronze_ingestion(
            target_month=target_month, target_year=target_year,
            force_reprocess=force_reprocess, execution_id=execution_id
        )

        # 2. Silver
        self.execute_silver_transformation(
            target_month=target_month, target_year=target_year, execution_id=execution_id
        )

        # 3. Gold
        gold_res = self.execute_gold_aggregation(
            target_month=target_month, target_year=target_year, execution_id=execution_id
        )

        # 4. Data Quality
        quality_res = self.execute_data_quality_audit(execution_id=execution_id)

        total_duration = time.time() - t0_total
        failed_partitions = bronze_res.get("failed_partitions", [])
        final_status = "completed" if len(failed_partitions) == 0 else "degraded"

        self._update_progress("Concluido", f"Pipeline {final_status} em {total_duration:.1f}s!", 100.0)

        logger.info("=" * 88)
        logger.info(f"PIPELINE QIMED LAKEHOUSE V3 CONCLUIDO ({final_status.upper()}) EM {total_duration:.2f}s!")
        logger.info("=" * 88)

        return {
            "id_execucao": execution_id,
            "status": final_status,
            "total_duration_seconds": round(total_duration, 2),
            "peak_rss_mb": bronze_res.get("peak_rss_mb", 0.0),
            "total_sih_rows": bronze_res.get("total_sih_rows", 0),
            "total_sia_rows": bronze_res.get("total_sia_rows", 0),
            "failed_partitions": failed_partitions,
            "gold_data_marts": gold_res.get("data_marts"),
            "quality_audit": quality_res.get("quality_audit"),
        }
