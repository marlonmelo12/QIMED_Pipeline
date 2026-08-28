"""
Observabilidade e Telemetria em Tempo Real - QIMED Lakehouse V3.
Mede o consumo real de mem?ria RAM (peak RSS) via psutil, uso de CPU e throughput de linhas/segundo.
"""
import os
import time
from typing import Any, Dict, Optional
import psutil
import pandas as pd
from deltalake.writer import write_deltalake

from src.utils.config_loader import load_pipeline_config
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class MetricsCollector:
    """
    Coletor de telemetria de hardware e throughput.
    """

    def __init__(self, system_dir: Optional[str] = None):
        cfg = load_pipeline_config()
        base_system = system_dir or cfg.get("paths", {}).get("system_dir", "lakehouse/system")
        self.metrics_table_path = os.path.join(base_system, "metrics")
        os.makedirs(self.metrics_table_path, exist_ok=True)
        self.process = psutil.Process(os.getpid())

    def get_current_metrics(self) -> Dict[str, Any]:
        """
        Obt?m m?tricas instant?neas reais do processo.
        """
        mem_info = self.process.memory_info()
        rss_mb = mem_info.rss / (1024 * 1024)
        cpu_pct = self.process.cpu_percent(interval=None)

        return {
            "current_rss_mb": round(rss_mb, 2),
            "cpu_pct": round(cpu_pct, 2),
            "threads_count": self.process.num_threads(),
        }

    def record_stage_metrics(
        self,
        execution_id: str,
        stage_name: str,
        duration_seconds: float,
        rows_processed: int,
        peak_rss_mb: float,
    ):
        """
        Registra as m?tricas de um est?gio na tabela Delta de m?tricas.
        """
        rows_per_sec = round(rows_processed / max(0.001, duration_seconds), 1)
        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        entry = {
            "id_execucao": str(execution_id),
            "estagio": str(stage_name),
            "duracao_segundos": float(round(duration_seconds, 2)),
            "linhas_processadas": int(rows_processed),
            "linhas_por_segundo": float(rows_per_sec),
            "peak_rss_mb": float(round(peak_rss_mb, 2)),
            "timestamp": str(now_str),
        }

        df = pd.DataFrame([entry])
        try:
            write_deltalake(self.metrics_table_path, df, mode="append", schema_mode="merge")
            logger.info(f"[METRICS] {stage_name}: {rows_processed:,} linhas em {duration_seconds:.2f}s ({rows_per_sec:,.0f} linhas/s). Peak RSS: {peak_rss_mb:.1f} MB.")
        except Exception as e:
            logger.warning(f"Falha ao gravar metricas Delta ({e}).")
