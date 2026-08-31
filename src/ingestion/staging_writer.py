"""
Parquet Staging Writer com Bounded Queue Real (Backpressure) - QIMED Lakehouse V3.
Implementa o padrão Producer-Consumer com fila limitada (maxsize=3)
para desacoplar a descompressão LZO/parsing de DBC do I/O de disco em Parquet.
"""
import os
import queue
import threading
from typing import Any, Dict, List, Optional, Callable, Generator
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)

_SENTINEL = object()


class ParquetStagingWriter:
    """
    Consumidor que grava Arrow RecordBatches em Parquet com Backpressure real.
    """

    def __init__(
        self,
        subsystem: str,
        year: int,
        month: int,
        uf: str,
        staging_root: Optional[str] = None,
        max_queue_size: int = 3,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.subsystem = subsystem.lower()
        self.year = year
        self.month = month
        self.uf = uf.upper()

        cfg = config or load_pipeline_config()
        base_staging = staging_root or cfg.get("paths", {}).get("staging_dir", "lakehouse/staging")
        self.staging_dir = os.path.join(
            base_staging,
            self.subsystem,
            f"year={self.year}",
            f"month={self.month:02d}",
            f"uf={self.uf}"
        )
        os.makedirs(self.staging_dir, exist_ok=True)

        self.max_queue_size = max_queue_size
        self.part_counter = 0
        self.staged_files: List[str] = []
        self.total_rows_staged = 0

    def write_batch(self, batch: pa.RecordBatch) -> str:
        """
        Grava um RecordBatch no disco e libera a memória do batch imediatamente.
        """
        self.part_counter += 1
        filename = f"part-{self.part_counter:06d}.parquet"
        filepath = os.path.join(self.staging_dir, filename)

        table = pa.Table.from_batches([batch])
        pq.write_table(table, filepath, compression="snappy")

        self.staged_files.append(filepath)
        self.total_rows_staged += len(batch)
        del table
        return filepath

    def process_batch_stream(self, batch_generator: Generator[pa.RecordBatch, None, None]) -> Dict[str, Any]:
        """
        Processa o gerador de batches utilizando Bounded Queue com Backpressure em thread dedicada.
        """
        q: queue.Queue = queue.Queue(maxsize=self.max_queue_size)
        producer_error = []

        def _producer():
            try:
                for batch in batch_generator:
                    q.put(batch)  # Bloqueia se a fila atingir maxsize (Backpressure)
                q.put(_SENTINEL)
            except Exception as ex:
                producer_error.append(ex)
                q.put(_SENTINEL)

        prod_thread = threading.Thread(target=_producer, name=f"producer_{self.subsystem}_{self.uf}", daemon=True)
        prod_thread.start()

        # Consumer loop na thread principal
        while True:
            item = q.get()
            if item is _SENTINEL:
                q.task_done()
                break
            self.write_batch(item)
            del item
            q.task_done()

        prod_thread.join()

        if producer_error:
            raise producer_error[0]

        logger.info(
            f"[STAGING] {self.subsystem.upper()}-{self.uf}: {len(self.staged_files)} arquivos gravados com Backpressure "
            f"({self.total_rows_staged:,} linhas)."
        )

        return {
            "staged_files": self.staged_files,
            "total_rows": self.total_rows_staged,
            "staging_dir": self.staging_dir,
        }

    def get_staged_files(self) -> List[str]:
        if not os.path.exists(self.staging_dir):
            return []
        return [
            os.path.join(self.staging_dir, f)
            for f in sorted(os.listdir(self.staging_dir))
            if f.endswith(".parquet")
        ]

    def cleanup_staging(self, preserve_on_failure: bool = False):
        if preserve_on_failure:
            return
        for f in self.get_staged_files():
            try:
                os.remove(f)
            except Exception as e:
                logger.warning(f"Falha ao remover arquivo de staging {f}: {e}")
        try:
            if os.path.exists(self.staging_dir) and not os.listdir(self.staging_dir):
                os.rmdir(self.staging_dir)
        except Exception:
            pass
