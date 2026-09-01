"""
DeltaBatchSink - QIMED Lakehouse V3 (Pure PyArrow Zero-Copy Delta Writer).
Consolida múltiplos arquivos de staging Parquet em um commit Delta Lake
atômico e idempotente, executando valida??es pré-commit e compactação por cardinalidade,
sem converter para Pandas.
"""
import gc
import os
import time
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake

from src.ingestion.staging_writer import ParquetStagingWriter
from src.ingestion.pre_commit_validator import PreCommitValidator
from src.ingestion.lock_manager import PartitionLockManager
from src.ingestion.manifest_delta import DeltaManifestManager
from src.lakehouse.compaction import DeltaCompactor
from src.observability.memory_governor import (
    MemoryGovernor,
    MemoryPressureError,
    calculate_dynamic_batch_size,
    is_memory_pressure_error,
)
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class DeltaBatchSink:

    """
    Sink atômico para persistência de dados de staging no Delta Lake Bronze.
    """

    def __init__(
        self,
        subsystem: str,
        year: int,
        month: int,
        uf: str,
        bronze_root: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.subsystem = subsystem.upper()
        self.year = year
        self.month = month
        self.uf = uf.upper()

        self.cfg = config or load_pipeline_config()
        base_bronze = bronze_root or self.cfg.get("paths", {}).get("bronze_dir", "lakehouse/bronze")
        self.target_delta_table = os.path.join(base_bronze, "datasus", self.subsystem.lower())

        self.validator = PreCommitValidator(subsystem=self.subsystem)
        self.lock_manager = PartitionLockManager()
        self.manifest_manager = DeltaManifestManager()
        self.compactor = DeltaCompactor(config=self.cfg)
        self.memory_governor = MemoryGovernor(config=self.cfg)


    def commit_staging_to_bronze(
        self,
        staging_writer: ParquetStagingWriter,
        execution_id: str,
        source_files: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Executa a persistência atômica da partição com Lock, Validação, Compactação e Commit.
        """
        partition_key = f"{self.subsystem}/{self.year}/{self.month:02d}/{self.uf}"
        start_time = time.time()

        staging_files = staging_writer.get_staged_files()
        if not staging_files:
            logger.warning(f"Nenhum arquivo de staging para commitar em {partition_key}.")
            return {"status": "skipped_empty", "total_rows": 0}

        with self.lock_manager.lock(partition_key, execution_id):
            # 1. Validação Pr?-Commit
            validation_res = self.validator.validate_staging_files(staging_files)
            if not validation_res["is_valid"]:
                err = validation_res.get("error", "Validacao pre-commit falhou")
                self.manifest_manager.record_manifest_entry(
                    subsystem=self.subsystem,
                    year=self.year,
                    month=self.month,
                    uf=self.uf,
                    files=source_files or staging_files,
                    total_rows=0,
                    status="failed",
                    execution_id=execution_id,
                    duration_seconds=time.time() - start_time,
                    error_message=err,
                )
                raise ValueError(f"Pre-Commit Validation Error em {partition_key}: {err}")

            total_rows = validation_res["total_rows"]

            # 2. Compactação Inteligente por Cardinalidade
            compacted_output_dir = os.path.join(staging_writer.staging_dir, "compacted")
            ready_files = self.compactor.compact_staging_files(staging_files, self.uf, compacted_output_dir)

            # 3. Gravação Delta Lake Atômica em Streaming Bounded (PyArrow RecordBatchReader)
            now_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            schema_ver = f"{self.subsystem}_v2026_01"

            # Governança de Threads para evitar saturação de CPU/Sockets
            max_threads = int(self.cfg.get("resources", {}).get("max_worker_threads", 4))
            try:
                pa.set_cpu_count(max_threads)
                pa.set_io_cpu_count(max_threads)
            except Exception:
                pass

            # Extrai schema base do primeiro arquivo
            sample_pq = pq.ParquetFile(ready_files[0])
            base_schema = sample_pq.schema_arrow
            del sample_pq

            technical_fields = [
                pa.field("fonte", pa.string()),
                pa.field("id_execucao", pa.string()),
                pa.field("data_ingestao", pa.string()),
                pa.field("versao_schema", pa.string()),
                pa.field("ano", pa.string()),
                pa.field("mes", pa.string()),
                pa.field("uf", pa.string()),
            ]
            enriched_schema = pa.schema(list(base_schema) + technical_fields)

            from src.ingestion.predicate_builder import build_partition_predicate
            predicate = build_partition_predicate(
                schema=enriched_schema,
                year=self.year,
                month=self.month,
                uf=self.uf,
            )


            target_batch_mb = float(self.cfg.get("staging", {}).get("enrichment_batch_target_mb", 64.0))
            min_rows = int(self.cfg.get("resources", {}).get("min_batch_rows", 10_000))

            max_rows = int(self.cfg.get("resources", {}).get("max_batch_rows", 250_000))

            def _generate_enriched_batches():
                for file_idx, f in enumerate(ready_files, 1):
                    pf = pq.ParquetFile(f, memory_map=False)
                    batch_size = calculate_dynamic_batch_size(
                        pf.metadata,
                        target_batch_mb=target_batch_mb,
                        min_rows=min_rows,
                        max_rows=max_rows,
                    )
                    logger.debug(
                        f"[DYNAMIC BATCH] Arquivo {file_idx}/{len(ready_files)} ({os.path.basename(f)}): "
                        f"batch_size={batch_size:,} linhas (alvo: {target_batch_mb:.0f}MB)."
                    )
                    try:
                        for batch in pf.iter_batches(batch_size=batch_size):
                            n = len(batch)
                            arrays = list(batch.columns) + [
                                pa.array(["DATASUS"] * n, pa.string()),
                                pa.array([str(execution_id)] * n, pa.string()),
                                pa.array([now_ts] * n, pa.string()),
                                pa.array([schema_ver] * n, pa.string()),
                                pa.array([str(self.year)] * n, pa.string()),
                                pa.array([f"{self.month:02d}"] * n, pa.string()),
                                pa.array([self.uf] * n, pa.string()),
                            ]
                            enriched_batch = pa.RecordBatch.from_arrays(arrays, schema=enriched_schema)
                            yield enriched_batch
                            del batch, arrays, enriched_batch
                    finally:
                        del pf
                        gc.collect()
                        try:
                            pa.default_memory_pool().release_unused()
                        except Exception:
                            pass
                        # Checkpoint de governança a cada arquivo
                        self.memory_governor.checkpoint(
                            context=f"{self.subsystem}/{self.year}/{self.month:02d}/{self.uf} (arquivo {file_idx}/{len(ready_files)})"
                        )
                        # Heartbeat ativo do lock de partição
                        self.lock_manager.heartbeat(partition_key=partition_key, execution_id=execution_id)

            reader = pa.RecordBatchReader.from_batches(enriched_schema, _generate_enriched_batches())

            from src.utils.s3_storage import get_s3_storage_options, resolve_lakehouse_path

            target_table = resolve_lakehouse_path(self.target_delta_table, default_layer="bronze")
            storage_opts = get_s3_storage_options(target_table)

            write_deltalake(
                target_table,
                reader,
                mode="overwrite",
                predicate=predicate,
                partition_by=["ano", "mes", "uf"],
                storage_options=storage_opts,
                schema_mode="merge",
            )
            del reader
            try:
                pa.default_memory_pool().release_unused()
            except Exception:
                pass

            duration = time.time() - start_time


            # 4. Limpa Staging com Segurança
            staging_writer.cleanup_staging(preserve_on_failure=False)
            if os.path.exists(compacted_output_dir):
                for cf in ready_files:
                    if os.path.exists(cf) and cf.startswith(compacted_output_dir):
                        try:
                            os.remove(cf)
                        except Exception:
                            pass
                try:
                    os.rmdir(compacted_output_dir)
                except Exception:
                    pass

            # 5. Registra Sucesso no Manifesto Delta
            self.manifest_manager.record_manifest_entry(
                subsystem=self.subsystem,
                year=self.year,
                month=self.month,
                uf=self.uf,
                files=source_files or staging_files,
                total_rows=total_rows,
                status="committed",
                execution_id=execution_id,
                duration_seconds=duration,
            )

            logger.info(
                f"[DELTA BRONZE COMMIT OK] {self.subsystem}-{self.uf}: {total_rows:,} linhas gravadas "
                f"com sucesso em {duration:.2f}s (Zero-Copy PyArrow)."
            )

            return {
                "status": "committed",
                "total_rows": total_rows,
                "duration_seconds": duration,
                "table_path": self.target_delta_table,
            }
