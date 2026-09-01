"""
Partition Lock Manager - QIMED Lakehouse V3.
Gerencia exclusão mútua e locks atômicos de partição para evitar
corrupção de dados por execuções concorrentes simultâneas.

Características de Hardening:
1. Aquisição Atômica via os.open(O_CREAT | O_EXCL) no Filesystem;
2. Reentrância segura no mesmo processo com contador de profundidade (lock_depth);
3. Heartbeat em background thread daemon enquanto o lock estiver ativo;
4. Detecção e takeover atômico de Stale Locks (após expiração comprovada do timeout);
5. Proteção contra Lost Ownership e remoção indevida por outros processos;
6. Exceção semântica dedicada: PartitionLockConflictError;
7. Quarentena de arquivos corrompidos sem destruição de evidências;
8. Cleanup gracioso em SIGTERM via handler no processo principal;
9. Observabilidade estruturada para auditoria forense.
"""
import os
import time
import json
import socket
import signal
import tempfile
import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional, Set

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)

DEFAULT_LOCK_TIMEOUT_SECONDS = 120  # 2 minutos de tolerância sem heartbeat
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15  # Renovação a cada 15s


class PartitionLockConflictError(Exception):
    """
    Lançada quando a partição está ativamente bloqueada por outro owner legítimo.
    Permite ao pipeline diferenciar conflito de concorrência de falhas graves de infraestrutura.
    """
    def __init__(self, partition_key: str, existing_owner: str, lock_age: float, timeout_seconds: float):
        self.partition_key = partition_key
        self.existing_owner = existing_owner
        self.lock_age = lock_age
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"[LOCK_CONFLICT] Nao foi possivel adquirir lock para {partition_key}. "
            f"Particao bloqueada pelo owner ativo '{existing_owner}' "
            f"(idade do lock: {lock_age:.1f}s / timeout: {timeout_seconds:.1f}s)."
        )


class PartitionLockCorruptedError(Exception):
    """
    Lançada quando o arquivo de lock existente está corrompido e não pôde ser reparado com segurança.
    """
    pass


# Registro global de managers ativos no processo para suporte a cleanup em SIGTERM
_ACTIVE_MANAGERS: Set["PartitionLockManager"] = set()
_ACTIVE_MANAGERS_LOCK = threading.Lock()
_SIGTERM_HANDLER_REGISTERED = False


def _sigterm_cleanup_handler(signum, frame):
    """Handler de encerramento gracioso em SIGTERM para liberar locks locais."""
    logger.warning(f"[SIGNAL] SIGTERM recebido (pid={os.getpid()}). Executando cleanup gracioso de locks...")
    with _ACTIVE_MANAGERS_LOCK:
        for mgr in list(_ACTIVE_MANAGERS):
            try:
                mgr.release_all_local_claims()
            except Exception as e:
                logger.error(f"[SIGNAL] Erro durante cleanup de locks: {e}")
    # Encerra o processo conforme o padrão Unix (128 + SIGTERM = 143)
    raise SystemExit(128 + signal.SIGTERM)


def register_sigterm_handler():
    """Registra o handler de SIGTERM na thread principal se ainda não registrado."""
    global _SIGTERM_HANDLER_REGISTERED
    if _SIGTERM_HANDLER_REGISTERED:
        return
    try:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, _sigterm_cleanup_handler)
            _SIGTERM_HANDLER_REGISTERED = True
            logger.debug("[SIGNAL] Handler de SIGTERM registrado com sucesso.")
    except Exception as e:
        logger.debug(f"[SIGNAL] Nao foi possivel registrar handler de SIGTERM (ambiente restrito/Windows): {e}")


class PartitionLockManager:
    """
    Gerenciador de locks atômicos por partição lógica (ex: SIA/2026/05/MG).
    """

    def __init__(
        self,
        locks_dir: Optional[str] = None,
        timeout_seconds: int = DEFAULT_LOCK_TIMEOUT_SECONDS,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ):
        cfg = load_pipeline_config()
        base_system = locks_dir or os.path.join(cfg.get("paths", {}).get("system_dir", "lakehouse/system"), "locks")
        self.locks_dir = base_system
        os.makedirs(self.locks_dir, exist_ok=True)
        self.timeout_seconds = timeout_seconds
        self.heartbeat_interval = heartbeat_interval

        # Controle local de reentrância por partição:
        # partition_key -> {"owner_token": str, "depth": int, "stop_event": threading.Event, "thread": threading.Thread}
        self._local_claims: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

        with _ACTIVE_MANAGERS_LOCK:
            _ACTIVE_MANAGERS.add(self)

        register_sigterm_handler()

    def _get_lock_filepath(self, partition_key: str) -> str:
        safe_key = partition_key.replace("/", "_").replace("\\", "_").replace("=", "_")
        return os.path.join(self.locks_dir, f"{safe_key}.lock")

    def _read_lock_data(self, lock_path: str) -> Optional[Dict[str, Any]]:
        """Lê e valida o payload JSON do arquivo de lock de forma segura."""
        if not os.path.exists(lock_path):
            return None
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return None
                return json.loads(content)
        except Exception:
            return None

    def _start_heartbeat(self, partition_key: str, owner_token: str) -> threading.Event:
        """Inicia uma thread daemon para renovar o timestamp do lock enquanto o processo estiver vivo."""
        stop_event = threading.Event()

        def _heartbeat_worker():
            logger.debug(f"[LOCK_HEARTBEAT_START] Thread de heartbeat iniciada para {partition_key} (owner: {owner_token}).")
            while not stop_event.is_set():
                # Aguarda o intervalo de heartbeat
                if stop_event.wait(timeout=self.heartbeat_interval):
                    break
                # Executa renovação
                try:
                    success = self.heartbeat(partition_key, owner_token)
                    if not success:
                        logger.warning(
                            f"[LOCK_LOST_OWNERSHIP] Heartbeat detectou perda de posse para {partition_key} "
                            f"(owner: {owner_token}). Interrompendo heartbeat."
                        )
                        break
                except Exception as ex:
                    logger.debug(f"[LOCK_HEARTBEAT_ERR] Erro transitório no heartbeat de {partition_key}: {ex}")
            logger.debug(f"[LOCK_HEARTBEAT_STOP] Thread de heartbeat encerrada para {partition_key}.")

        t = threading.Thread(
            target=_heartbeat_worker,
            name=f"LockHeartbeat-{partition_key.replace('/', '_')}",
            daemon=True,
        )
        t.start()
        return stop_event

    def acquire_lock(self, partition_key: str, execution_id: str, timeout_seconds: Optional[int] = None) -> bool:
        """
        Tenta adquirir o lock atômico da partição.
        Suporta reentrância se chamado pelo mesmo owner no mesmo processo.
        """
        eff_timeout = timeout_seconds or self.timeout_seconds
        lock_path = self._get_lock_filepath(partition_key)
        now = time.time()

        with self._lock:
            # 1. Checagem de Reentrância Local (Mesmo processo)
            if partition_key in self._local_claims:
                claim = self._local_claims[partition_key]
                if claim.get("owner_token") == execution_id:
                    claim["depth"] += 1
                    logger.info(
                        f"[LOCK_REENTRANT] Reentrancia concedida para {partition_key} "
                        f"(owner: {execution_id}, depth: {claim['depth']})."
                    )
                    return True

            payload = {
                "partition_key": partition_key,
                "owner_token": execution_id,
                "execution_id": execution_id,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "timestamp": now,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                "heartbeat_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                "timeout_seconds": eff_timeout,
            }

            # 2. Tentativa de Criação Atômica Exclusiva
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                
                # Inicia heartbeat e registra claim local
                stop_ev = self._start_heartbeat(partition_key, execution_id)
                self._local_claims[partition_key] = {
                    "owner_token": execution_id,
                    "depth": 1,
                    "stop_event": stop_ev,
                }
                logger.info(
                    f"[LOCK_ACQUIRED] Lock obtido com sucesso para {partition_key} "
                    f"(owner: {execution_id}, timeout: {eff_timeout}s)."
                )
                return True
            except FileExistsError:
                pass

            # 3. Trata Arquivo Já Existente (Lock Ativo vs Stale vs Corrompido)
            lock_data = self._read_lock_data(lock_path)
            
            # 3a. Trata arquivo corrompido (JSON inválido ou vazio)
            if not lock_data or not isinstance(lock_data, dict) or "timestamp" not in lock_data:
                logger.error(
                    f"[LOCK_CORRUPTED] Arquivo de lock corrompido em {lock_path}. "
                    f"Movendo para quarentena e tentando reaproveitar..."
                )
                quarantine_path = f"{lock_path}.corrupted_{int(now)}"
                try:
                    os.replace(lock_path, quarantine_path)
                    return self.acquire_lock(partition_key, execution_id, eff_timeout)
                except Exception as e:
                    logger.error(f"[LOCK_CORRUPTED_ERR] Falha ao mover arquivo corrompido: {e}")
                    return False

            existing_owner = lock_data.get("owner_token") or lock_data.get("execution_id", "unknown")
            lock_time = float(lock_data.get("timestamp", 0))
            lock_age = now - lock_time

            # 3b. Reentrância entre instâncias com mesmo owner token
            if existing_owner == execution_id:
                stop_ev = self._start_heartbeat(partition_key, execution_id)
                self._local_claims[partition_key] = {
                    "owner_token": execution_id,
                    "depth": 1,
                    "stop_event": stop_ev,
                }
                logger.info(
                    f"[LOCK_REENTRANT] Reconhecido owner identico ({execution_id}) para {partition_key}."
                )
                return True

            # 3c. Lock Ativo mantido por outro owner (Rejeição de Concorrência)
            if lock_age < eff_timeout:
                logger.warning(
                    f"[LOCK_CONFLICT] Particao {partition_key} esta bloqueada pelo owner "
                    f"'{existing_owner}' desde {lock_data.get('created_at')} "
                    f"(ultimo heartbeat: {lock_data.get('heartbeat_at', 'N/A')}, idade: {lock_age:.1f}s < {eff_timeout}s)."
                )
                return False

            # 3d. Lock Stale Comprovado (Idade > Timeout) -> Takeover Atômico
            logger.warning(
                f"[LOCK_STALE_TAKEOVER] Lock expirado detectado em {partition_key} "
                f"(owner anterior: '{existing_owner}', idade: {lock_age:.1f}s >= {eff_timeout}s). "
                f"Executando takeover atomico para owner '{execution_id}'..."
            )
            try:
                tmp_fd, tmp_path = tempfile.mkstemp(dir=self.locks_dir, prefix=".lock_takeover_")
                try:
                    with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                        json.dump(payload, f)
                    os.replace(tmp_path, lock_path)
                    
                    stop_ev = self._start_heartbeat(partition_key, execution_id)
                    self._local_claims[partition_key] = {
                        "owner_token": execution_id,
                        "depth": 1,
                        "stop_event": stop_ev,
                    }
                    logger.info(
                        f"[LOCK_ACQUIRED] Takeover concluido com sucesso para {partition_key} (owner: {execution_id})."
                    )
                    return True
                except Exception:
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                    raise
            except Exception as e:
                logger.error(f"[LOCK_TAKEOVER_ERR] Falha ao executar takeover atomico para {partition_key}: {e}")
                return False

    def heartbeat(self, partition_key: str, execution_id: str) -> bool:
        """
        Renova o timestamp do lock atomicamente via os.replace(),
        garantindo Lost Ownership Protection (verifica se o owner ainda é o mesmo).
        """
        lock_path = self._get_lock_filepath(partition_key)
        if not os.path.exists(lock_path):
            return False

        try:
            lock_data = self._read_lock_data(lock_path)
            if not lock_data:
                return False

            curr_owner = lock_data.get("owner_token") or lock_data.get("execution_id")
            if curr_owner != execution_id:
                logger.warning(
                    f"[LOCK_HEARTBEAT_REJECTED] Lock de {partition_key} pertence a outro owner ({curr_owner} != {execution_id})."
                )
                return False

            now = time.time()
            payload = dict(lock_data)
            payload["timestamp"] = now
            payload["heartbeat_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))

            tmp_fd, tmp_path = tempfile.mkstemp(dir=self.locks_dir, prefix=".lock_hb_")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                os.replace(tmp_path, lock_path)
                logger.debug(f"[LOCK_HEARTBEAT] Lock renovado com sucesso para {partition_key} (owner: {execution_id}).")
                return True
            except Exception:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.debug(f"[LOCK_HEARTBEAT_ERR] Falha ao executar heartbeat para {partition_key}: {e}")
            return False

    def release_lock(self, partition_key: str, execution_id: str):
        """
        Libera o lock da partição de forma segura:
        1. Decrementa o lock_depth da reentrância local;
        2. Encerra a thread de heartbeat;
        3. Remove o arquivo físico somente quando depth == 0 e comprovando a posse do owner.
        """
        lock_path = self._get_lock_filepath(partition_key)
        with self._lock:
            if partition_key in self._local_claims:
                claim = self._local_claims[partition_key]
                if claim.get("owner_token") == execution_id:
                    claim["depth"] -= 1
                    if claim["depth"] > 0:
                        logger.info(
                            f"[LOCK_REENTRANT_EXIT] Saindo de bloco interno para {partition_key} "
                            f"(owner: {execution_id}, depth restante: {claim['depth']}). Arquivo mantido."
                        )
                        return

                    # depth == 0: Encerra heartbeat e remove claim local
                    claim["stop_event"].set()
                    del self._local_claims[partition_key]

            # Liberação física no Filesystem
            if os.path.exists(lock_path):
                try:
                    lock_data = self._read_lock_data(lock_path)
                    if lock_data:
                        file_owner = lock_data.get("owner_token") or lock_data.get("execution_id")
                        if file_owner == execution_id:
                            os.remove(lock_path)
                            logger.info(f"[LOCK_RELEASED] Lock liberado com sucesso para {partition_key} (owner: {execution_id}).")
                        else:
                            logger.warning(
                                f"[LOCK_RELEASE_DENIED] Tentativa de liberar lock pertencente a outro owner "
                                f"({file_owner} != {execution_id}). Arquivo preservado."
                            )
                    else:
                        # Arquivo vazio ou corrompido
                        os.remove(lock_path)
                except Exception as e:
                    logger.warning(f"[LOCK_RELEASE_ERR] Falha ao liberar lock em {lock_path}: {e}")

    def release_all_local_claims(self):
        """Libera todos os locks mantidos ativamente por esta instância no processo corrente."""
        with self._lock:
            for partition_key, claim in list(self._local_claims.items()):
                owner = claim.get("owner_token")
                claim["depth"] = 0
                claim["stop_event"].set()
                lock_path = self._get_lock_filepath(partition_key)
                if os.path.exists(lock_path):
                    try:
                        lock_data = self._read_lock_data(lock_path)
                        if lock_data and (lock_data.get("owner_token") == owner or lock_data.get("execution_id") == owner):
                            os.remove(lock_path)
                            logger.info(f"[LOCK_RELEASED_CLEANUP] Lock de {partition_key} liberado no cleanup.")
                    except Exception:
                        pass
            self._local_claims.clear()

    @contextmanager
    def lock(self, partition_key: str, execution_id: str, timeout_seconds: Optional[int] = None):
        """
        Context manager para aquisição e liberação segura e reentrante do lock.
        Lança PartitionLockConflictError em caso de conflito com outro owner ativo.
        """
        acquired = self.acquire_lock(partition_key, execution_id, timeout_seconds=timeout_seconds)
        if not acquired:
            lock_path = self._get_lock_filepath(partition_key)
            lock_data = self._read_lock_data(lock_path) or {}
            existing_owner = lock_data.get("owner_token") or lock_data.get("execution_id", "unknown")
            lock_time = float(lock_data.get("timestamp", 0))
            lock_age = time.time() - lock_time if lock_time > 0 else 0.0
            eff_timeout = timeout_seconds or self.timeout_seconds
            raise PartitionLockConflictError(partition_key, existing_owner, lock_age, eff_timeout)
        try:
            yield
        finally:
            self.release_lock(partition_key, execution_id)
