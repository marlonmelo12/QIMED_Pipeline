"""
Partition Lock Manager - QIMED Lakehouse V3.
Gerencia exclusão mútua e locks atômicos de partição para evitar
corrupção de dados por execuções concorrentes simultâneas.
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
    Gerenciador de locks atômicos por partição lógica (ex: SIA/2026/05/MG).
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
                    # [V2-07] Janela de race fechada: escreve em arquivo temporario
                    # e substitui o lock expirado de forma atomica via os.replace().
                    # os.replace() e atomico em Windows e Unix — nao ha janela entre
                    # remover o antigo e criar o novo como havia com os.remove + O_EXCL.
                    import tempfile
                    try:
                        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.locks_dir, prefix=".lock_tmp_")
                        try:
                            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                                json.dump(payload, f)
                            os.replace(tmp_path, lock_path)
                            logger.info(f"[LOCK ACQUIRED] Lock renovado atomicamente para {partition_key} (execucao: {execution_id}).")
                            return True
                        except Exception:
                            # Limpar temp se replace falhar
                            try:
                                os.remove(tmp_path)
                            except OSError:
                                pass
                            raise
                    except Exception:
                        logger.warning(
                            f"[LOCK RACE] Outro processo adquiriu o lock de {partition_key} concorrentemente."
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
        Libera o lock da partição de forma segura.
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
