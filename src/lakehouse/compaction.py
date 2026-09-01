"""
Compactor Inteligente Orientado à Cardinalidade - QIMED Lakehouse V3.
Consolida múltiplos arquivos de staging Parquet em um número ideal de arquivos
balanceados por UF antes do commit final utilizando streaming out-of-core (Arrow Dataset).
"""
import os
import time
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class DeltaCompactor:
    """
    Consolida arquivos Parquet baseado na cardinalidade da UF para evitar small files
    utilizando streaming estritamente Bounded-Memory.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        self.compaction_cfg = self.cfg.get("compaction", {})
        self.resources_cfg = self.cfg.get("resources", {})
        self.small_ufs = set(self.compaction_cfg.get("small_uf_list", ["AC", "AP", "RR", "RO", "TO", "SE"]))
        self.large_ufs = set(self.compaction_cfg.get("large_uf_list", ["SP"]))
        self.batch_size = int(self.resources_cfg.get("batch_chunk_rows", 250_000))
        self.max_threads = int(self.resources_cfg.get("max_worker_threads", 4))

    def get_target_files_count(self, uf: str, total_rows: int) -> int:
        """
        Determina o número ideal de arquivos para a partição.
        """
        uf_upper = uf.upper()
        if uf_upper in self.small_ufs or total_rows <= 300000:
            return 1
        elif uf_upper in self.large_ufs or total_rows >= 10000000:
            return 8
        else:
            return 3

    def compact_staging_files(self, staging_files: List[str], uf: str, output_dir: str) -> List[str]:
        """
        Lê múltiplos arquivos Parquet de staging e grava os arquivos consolidados balanceados
        em streaming bounded-memory sem materialização global em RAM.
        """
        if not staging_files:
            return []

        if len(staging_files) == 1:
            return staging_files

        t0 = time.perf_counter()

        # Governança de threads PyArrow
        try:
            pa.set_cpu_count(self.max_threads)
            pa.set_io_cpu_count(self.max_threads)
        except Exception:
            pass

        dataset = ds.dataset(staging_files, format="parquet")
        total_rows = dataset.count_rows()

        target_files_count = self.get_target_files_count(uf, total_rows)
        if target_files_count >= len(staging_files):
            return staging_files

        os.makedirs(output_dir, exist_ok=True)
        scanner_batch_size = min(self.batch_size, 50_000) if len(dataset.schema) > 30 else self.batch_size
        scanner = dataset.scanner(batch_size=scanner_batch_size)
        compacted_files = []


        if target_files_count == 1:
            out_name = "compacted-part-0001.parquet"
            out_path = os.path.join(output_dir, out_name)
            with pq.ParquetWriter(out_path, dataset.schema, compression="snappy") as writer:
                for batch in scanner.to_batches():
                    writer.write_batch(batch)
                    del batch
            compacted_files.append(out_path)
        else:
            rows_per_file = max(1, total_rows // target_files_count)
            file_idx = 1
            current_file_rows = 0
            out_name = f"compacted-part-{file_idx:04d}.parquet"
            out_path = os.path.join(output_dir, out_name)
            writer = pq.ParquetWriter(out_path, dataset.schema, compression="snappy")
            compacted_files.append(out_path)

            for batch in scanner.to_batches():
                if current_file_rows >= rows_per_file and file_idx < target_files_count:
                    writer.close()
                    file_idx += 1
                    current_file_rows = 0
                    out_name = f"compacted-part-{file_idx:04d}.parquet"
                    out_path = os.path.join(output_dir, out_name)
                    writer = pq.ParquetWriter(out_path, dataset.schema, compression="snappy")
                    compacted_files.append(out_path)

                writer.write_batch(batch)
                current_file_rows += batch.num_rows
                del batch

            writer.close()

        # Liberação ativa de memória nativa do pool Arrow
        del dataset, scanner
        try:
            pa.default_memory_pool().release_unused()
        except Exception:
            pass

        dur = time.perf_counter() - t0
        logger.info(
            f"[COMPACTION] UF={uf} input_files={len(staging_files)} "
            f"output_files={len(compacted_files)} rows={total_rows:,} duration={dur:.2f}s"
        )
        return compacted_files

