"""
DeltaBatchSink - QIMED Lakehouse V3 (Pure PyArrow Zero-Copy Delta Writer).
Consolida múltiplos arquivos de staging Parquet em um commit Delta Lake
atômico e idempotente, executando valida??es pré-commit e compactação por cardinalidade,
sem converter para Pandas.
"""
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

            # 3. Gravação Delta Lake Atômica em PyArrow (Commit Único ACID com Overwrite por Partição)
            now_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            schema_ver = f"{self.subsystem}_v2026_01"
            predicate = f"ano = '{self.year}' AND mes = '{self.month:02d}' AND uf = '{self.uf}'"

            all_tables = []
            for f in ready_files:
                tbl = pq.ParquetFile(f).read()
                n = len(tbl)

                # Injeta metadados técnicos nativos em PyArrow
                tbl = tbl.append_column("fonte", pa.array(["DATASUS"] * n, pa.string()))
                tbl = tbl.append_column("id_execucao", pa.array([str(execution_id)] * n, pa.string()))
                tbl = tbl.append_column("data_ingestao", pa.array([now_ts] * n, pa.string()))
                tbl = tbl.append_column("versao_schema", pa.array([schema_ver] * n, pa.string()))
                tbl = tbl.append_column("ano", pa.array([str(self.year)] * n, pa.string()))
                tbl = tbl.append_column("mes", pa.array([f"{self.month:02d}"] * n, pa.string()))
                tbl = tbl.append_column("uf", pa.array([self.uf] * n, pa.string()))
                all_tables.append(tbl)

            combined_table = pa.concat_tables(all_tables, promote_options="permissive") if len(all_tables) > 1 else all_tables[0]

            write_deltalake(
                self.target_delta_table,
                combined_table,
                mode="overwrite",
                predicate=predicate,
                partition_by=["ano", "mes", "uf"],
                schema_mode="merge",
            )
            del all_tables, combined_table

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
