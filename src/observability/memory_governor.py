"""
Memory Governor - QIMED Lakehouse V3.
Guardião proativo de memória para prevenção de OOM (Out-of-Memory).
Monitora o consumo de RSS do processo, detecta limites reais de container/cgroups
e executa liberação forçada de buffers nativos antes de levantar MemoryPressureError.
"""
import gc
import os
import re
from typing import Any, Dict, Optional, Union
import psutil
import pyarrow as pa

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class MemoryPressureError(Exception):
    """
    Exceção levantada quando o RSS do processo ultrapassa o limite seguro de governança
    mesmo após a tentativa de coleta de lixo e liberação do memory pool do Arrow.
    """
    pass


def parse_memory_limit_to_mb(limit_val: Union[str, int, float, None]) -> float:
    """
    Converte strings de configuração como '8GB', '4096MB', '10G' ou números brutos para Megabytes (MB).
    """
    if limit_val is None:
        return 8192.0

    if isinstance(limit_val, (int, float)):
        return float(limit_val)

    val_str = str(limit_val).strip().upper()
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([A-Z]+)?$", val_str)
    if not match:
        return 8192.0

    num = float(match.group(1))
    unit = match.group(2) or "MB"

    if unit in ("GB", "G", "GIB"):
        return num * 1024.0
    elif unit in ("MB", "M", "MIB"):
        return num
    elif unit in ("KB", "K", "KIB"):
        return num / 1024.0
    elif unit in ("B", "BYTES"):
        return num / (1024.0 * 1024.0)

    return num


def get_cgroup_memory_limit_mb(
    cgroup_v2_path: str = "/sys/fs/cgroup/memory.max",
    cgroup_v1_path: str = "/sys/fs/cgroup/memory/memory.limit_in_bytes",
) -> Optional[float]:
    """
    Lê o limite real de memória imposto pelo Linux/Docker Cgroups se estiver em container.
    Retorna None se for 'max', sentinela ilimitado ou se o arquivo não existir.
    """
    # Cgroups v2
    if os.path.exists(cgroup_v2_path):
        try:
            with open(cgroup_v2_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content != "max" and content.isdigit():
                    bytes_val = int(content)
                    return bytes_val / (1024.0 * 1024.0)
        except Exception:
            pass

    # Cgroups v1
    if os.path.exists(cgroup_v1_path):
        try:
            with open(cgroup_v1_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content.isdigit():
                    bytes_val = int(content)
                    # Valores sentinelas como 9223372036854771712 indicam 'sem limite'
                    if bytes_val < (1 << 60):
                        return bytes_val / (1024.0 * 1024.0)
        except Exception:
            pass

def calculate_dynamic_batch_size(
    metadata: Any,
    target_batch_mb: float = 64.0,
    min_rows: int = 10_000,
    max_rows: int = 250_000,
) -> int:
    """
    Calcula dinamicamente a contagem ideal de linhas por lote com base no número de colunas
    e no volume real de bytes dos dados em RAM (incluindo buffers nativos do Arrow).
    """
    if metadata is None:
        return min_rows

    num_rows = getattr(metadata, "num_rows", 0)
    num_cols = getattr(metadata, "num_columns", 15)

    # Suporte para dicionários / objetos mockados em testes
    if isinstance(metadata, dict):
        num_rows = metadata.get("num_rows", num_rows)
        num_cols = metadata.get("num_columns", num_cols)
        total_data_bytes = metadata.get("total_byte_size", metadata.get("serialized_size", 0))
    else:
        total_data_bytes = 0
        if hasattr(metadata, "num_row_groups") and hasattr(metadata, "row_group"):
            try:
                total_data_bytes = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
            except Exception:
                total_data_bytes = 0
        if total_data_bytes == 0:
            total_data_bytes = getattr(metadata, "total_byte_size", getattr(metadata, "serialized_size", 0))

    if num_rows <= 0 or total_data_bytes <= 0:
        return min_rows

    avg_bytes_per_row = float(total_data_bytes) / float(num_rows)
    # Se a tabela tiver muitas colunas (> 30 colunas), garanta que a estimativa considere o overhead de ponteiros/offsets
    if num_cols > 30:
        avg_bytes_per_row = max(avg_bytes_per_row, float(num_cols * 32.0))

    if avg_bytes_per_row <= 0:
        return min_rows

    target_bytes = target_batch_mb * 1024.0 * 1024.0
    calculated_rows = int(target_bytes / avg_bytes_per_row)

    return max(min_rows, min(max_rows, calculated_rows))



def is_memory_pressure_error(exc: Optional[BaseException]) -> bool:
    """
    Inspeciona recursivamente a cadeia de exceções (__cause__, __context__) para determinar
    com precisão forense se a falha foi causada por exaustão/pressão de memória (OOM / [Errno 12]),
    diferenciando de outros erros do Delta Lake / S3 / Schema.
    """
    if exc is None:
        return False

    visited = set()
    current = exc

    mem_keywords = (
        "cannot allocate memory",
        "out of memory",
        "out_of_memory",
        "bad_alloc",
        "std::bad_alloc",
        "allocation failed",
        "errno 12",
        "enomem",
        "memorypressureerror",
    )

    while current is not None and id(current) not in visited:
        visited.add(id(current))

        if isinstance(current, (MemoryPressureError, MemoryError)):
            return True

        if getattr(current, "errno", None) == 12:
            return True

        msg = str(current).lower()
        if any(kw in msg for kw in mem_keywords):
            return True

        current = current.__cause__ or current.__context__

    return False


class MemoryGovernor:

    """
    Monitor e controlador de consumo de memória para pipelines de dados em streaming.
    Calcula o soft limit a partir do menor valor entre o limite real do Cgroup do container
    e o teto configurado no pipeline.yaml, garantindo governança estrita e determinística.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        soft_limit_mb: Optional[float] = None,
        pressure_threshold: Optional[float] = None,
    ):
        self.cfg = config or load_pipeline_config()
        self.process = psutil.Process(os.getpid())

        if soft_limit_mb is not None:
            self.soft_limit_mb = float(soft_limit_mb)
        else:
            # 1. Configuração do YAML (DuckDB memory limit como baseline analítico)
            duckdb_limit = self.cfg.get("duckdb", {}).get("memory_limit", "8GB")
            yaml_limit_mb = parse_memory_limit_to_mb(duckdb_limit)

            # 2. Limite real do Container / Cgroup se presente
            cgroup_limit_mb = get_cgroup_memory_limit_mb()

            # 3. Limite de RAM física do Sistema
            sys_ram_mb = psutil.virtual_memory().total / (1024.0 * 1024.0)

            # Usa o menor limite válido para nunca exceder a capacidade real do container
            valid_limits = [yaml_limit_mb, sys_ram_mb]
            if cgroup_limit_mb is not None:
                valid_limits.append(cgroup_limit_mb)

            self.soft_limit_mb = min(valid_limits)

        if pressure_threshold is not None:
            self.pressure_threshold = float(pressure_threshold)
        else:
            self.pressure_threshold = float(
                self.cfg.get("resources", {}).get("memory_pressure_threshold", 0.85)
            )

        self.threshold_mb = self.soft_limit_mb * self.pressure_threshold

    def get_current_rss_mb(self) -> float:
        """Retorna o RSS atual do processo em Megabytes."""
        try:
            return self.process.memory_info().rss / (1024.0 * 1024.0)
        except Exception:
            return 0.0

    def checkpoint(
        self,
        context: Optional[str] = None,
        current_rss_mb: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Avalia o RSS do processo. Aceita current_rss_mb injetável para testes determinísticos.
        Se ultrapassar o threshold de pressão, força a liberação do memory pool do Arrow
        e gc.collect(). Se a pressão persistir, levanta MemoryPressureError.
        """
        rss = current_rss_mb if current_rss_mb is not None else self.get_current_rss_mb()

        if rss >= self.threshold_mb:
            ctx_str = f" ao processar {context}" if context else ""
            logger.warning(
                f"[MEMORY GOVERNOR] Pressao de memoria detectada{ctx_str}: "
                f"RSS={rss:.1f}MB >= {self.threshold_mb:.1f}MB "
                f"({self.pressure_threshold * 100:.0f}% de {self.soft_limit_mb:.0f}MB). "
                f"Forcando liberacao do pool nativo..."
            )

            # Força limpeza do Python e do pool C++ Arrow
            gc.collect()
            try:
                pa.default_memory_pool().release_unused()
            except Exception:
                pass

            # Se não foi injetado para teste fixo, re-mede após desalocação
            if current_rss_mb is None:
                rss_post = self.get_current_rss_mb()
            else:
                rss_post = current_rss_mb

            if rss_post >= self.threshold_mb:
                err_msg = (
                    f"MemoryPressureError: RSS {rss_post:.1f}MB excede "
                    f"{self.pressure_threshold * 100:.0f}% do limite de {self.soft_limit_mb:.0f}MB{ctx_str}"
                )
                logger.error(f"[MEMORY GOVERNOR VIOLATION] {err_msg}")
                raise MemoryPressureError(err_msg)
            else:
                logger.info(
                    f"[MEMORY GOVERNOR] Memoria recuperada com sucesso: "
                    f"RSS caiu de {rss:.1f}MB para {rss_post:.1f}MB."
                )
                rss = rss_post

        return {
            "status": "ok",
            "rss_mb": rss,
            "threshold_mb": self.threshold_mb,
            "soft_limit_mb": self.soft_limit_mb,
        }
