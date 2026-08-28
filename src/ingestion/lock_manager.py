"""
Partition Lock Manager - QIMED Lakehouse V3.
Gerencia exclus?o m?tua e locks at?micos de parti??o para evitar
corrup??o de dados por execu??es concorrentes simult?neas.
"""
import os
import time
import json
from contextlib import contextmanager
from typing import Optional

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class PartitionLockManager:
    """
    Gerenciador de locks at?micos por parti??o l?gica (ex: SIA/2026/05/MG).
    """

    def __init__(self, locks_dir: Optional[str] = None, timeout_seconds: int = 1800):
        cfg = load_pipeline_config()
        base_system = locks_dir or os.path.join(cfg.get("paths", {}).get("system_dir", "lakehouse/system"), "locks")
        self.locks_dir = base_system
        os.makedirs(self.locks_dir, exist_ok=True)
        self.timeout_seconds = timeout_seconds

    def _get_lock_filepath(self, partition_key: str) -> str:
        safe_key = partition_key.replace("/", "_").replace("\\", "_").replace("=", "_")
        return os.path.join(self.locks_dir, f"{safe_key}.lock")

    def acquire_lock(self, partition_key: str, execution_id: str) -> bool:
        """
        Tenta adquirir o lock atômico da partição.
        """
        lock_path = self._get_lock_filepath(partition_key)
        now = time.time()
        payload = {
            "partition_key": partition_key,
            "execution_id": execution_id,
            "timestamp": now,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        }

        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            logger.info(f"[LOCK ACQUIRED] Lock obtido para {partition_key} (execucao: {execution_id}).")
            return True
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    lock_data = json.load(f)

                # Reentrância: Se o lock já pertence ao mesmo execution_id, permite reentrar
                if lock_data.get("execution_id") == execution_id:
                    return True

                lock_time = lock_data.get("timestamp", 0)
                if (now - lock_time) < self.timeout_seconds:
                    logger.warning(
                        f"[LOCK ACTIVE] Particao {partition_key} esta bloqueada pela execucao "
                        f"{lock_data.get('execution_id')} desde {lock_data.get('created_at')}."
                    )
                    return False
                else:
                    logger.warning(f"[LOCK EXPIRED] Lock da particao {partition_key} expirou (> {self.timeout_seconds}s). Liberando...")
                    try:
                        os.remove(lock_path)
                    except Exception:
                        pass
                    
                    # Tenta adquirir atomicamente após remoção do lock expirado
                    try:
                        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        with os.fdopen(fd, "w", encoding="utf-8") as f:
                            json.dump(payload, f)
                        logger.info(f"[LOCK ACQUIRED] Lock obtido para {partition_key} (execucao: {execution_id}).")
                        return True
                    except FileExistsError:
                        logger.warning(
                            f"[LOCK RACE] Outro processo adquiriu o lock da particao {partition_key} concorrentemente."
                        )
                        return False
            except Exception as e:
                logger.error(f"Falha ao validar lock existente para {partition_key}: {e}")
                return False
        except Exception as e:
            logger.error(f"Falha ao gravar lock para {partition_key}: {e}")
            return False

    def release_lock(self, partition_key: str, execution_id: str):
        """
        Libera o lock da parti??o de forma segura.
        """
        lock_path = self._get_lock_filepath(partition_key)
        if os.path.exists(lock_path):
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    lock_data = json.load(f)
                if lock_data.get("execution_id") == execution_id:
                    os.remove(lock_path)
                    logger.info(f"[LOCK RELEASED] Lock liberado para {partition_key}.")
                else:
                    logger.warning(f"Tentativa de liberar lock de outro execution_id ({lock_data.get('execution_id')})")
            except Exception as e:
                logger.warning(f"Falha ao liberar lock {lock_path}: {e}")

    @contextmanager
    def lock(self, partition_key: str, execution_id: str):
        acquired = self.acquire_lock(partition_key, execution_id)
        if not acquired:
            raise RuntimeError(f"Nao foi possivel adquirir lock para {partition_key} (em uso por outra execucao).")
        try:
            yield
        finally:
            self.release_lock(partition_key, execution_id)
