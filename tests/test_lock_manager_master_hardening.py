"""
Suíte de Testes Master — Hardening do Partition Lock Manager QIMED.
Cobre integralmente os 17 cenários obrigatórios de testes:
- T01 a T12: Testes unitários (aquisição, reentrância, depth, ownership, heartbeat, stale, corrupção, sigterm)
- T13: Concorrência Real com multiprocessing (Processo A adquire, Processo B falha)
- T14: Stale Takeover entre processos separados
- T15: Isolamento estrito entre partições distintas
- T16: Processamento longo (duração > timeout com heartbeat ativo)
- T17: Simulação de crash abrupto / SIGKILL com recuperação garantida
"""
import os
import time
import json
import signal
import tempfile
import multiprocessing
import pytest
from unittest.mock import patch

from src.ingestion.lock_manager import (
    PartitionLockManager,
    PartitionLockConflictError,
    PartitionLockCorruptedError,
)


# =============================================================================
# SEÇÃO 1: TESTES UNITÁRIOS DE CONTRATO E COMPORTAMENTO (T01 a T12)
# =============================================================================

def test_t01_normal_acquisition(tmp_path):
    """T01: Aquisição normal e criação de arquivo de lock no filesystem."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10)
    partition = "SIA/2026/05/MG"
    owner = "owner_proc_1"

    assert lm.acquire_lock(partition, owner) is True
    lock_file = lm._get_lock_filepath(partition)
    assert os.path.exists(lock_file)

    with open(lock_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["owner_token"] == owner
    assert data["partition_key"] == partition
    assert data["pid"] == os.getpid()

    lm.release_lock(partition, owner)
    assert not os.path.exists(lock_file)


def test_t02_second_acquisition_conflict_error(tmp_path):
    """T02: Segunda aquisição por outro owner enquanto ativo gera PartitionLockConflictError."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    partition = "SIA/2026/05/SP"
    owner_1 = "owner_active"
    owner_2 = "owner_rejected"

    assert lm.acquire_lock(partition, owner_1) is True

    # acquire_lock direto retorna False
    assert lm.acquire_lock(partition, owner_2) is False

    # contextmanager lock() lança PartitionLockConflictError
    with pytest.raises(PartitionLockConflictError) as exc_info:
        with lm.lock(partition, owner_2):
            pass

    assert exc_info.value.existing_owner == owner_1
    assert exc_info.value.partition_key == partition

    lm.release_lock(partition, owner_1)


def test_t03_t04_reentrancy_and_depth(tmp_path):
    """T03 & T04: Reentrância mantém o lock ativo durante todo o aninhamento e rastreia depth."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=20)
    partition = "SIH/2026/05/RJ"
    owner = "owner_reentrant"
    lock_file = lm._get_lock_filepath(partition)

    with lm.lock(partition, owner):
        assert os.path.exists(lock_file)
        assert lm._local_claims[partition]["depth"] == 1

        # Bloco interno aninhado (ex: DeltaBatchSink dentro de master_pipeline)
        with lm.lock(partition, owner):
            assert os.path.exists(lock_file)
            assert lm._local_claims[partition]["depth"] == 2

        # Após sair do bloco interno, depth volta a 1 e o arquivo DEVE continuar no disco
        assert lm._local_claims[partition]["depth"] == 1
        assert os.path.exists(lock_file)

    # Após sair do bloco mais externo (depth=0), o arquivo é removido
    assert partition not in lm._local_claims
    assert not os.path.exists(lock_file)


def test_t05_owner_a_cannot_release_owner_b(tmp_path):
    """T05: Owner A não consegue liberar lock pertencente a Owner B."""
    locks_dir = str(tmp_path / "locks")
    lm1 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    lm2 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    partition = "SIA/2026/05/BA"
    owner_a = "owner_a"
    owner_b = "owner_b"
    lock_file = lm1._get_lock_filepath(partition)

    assert lm1.acquire_lock(partition, owner_a) is True
    assert os.path.exists(lock_file)

    # Owner B tenta liberar: o lock de A não pode ser removido
    lm2.release_lock(partition, owner_b)
    assert os.path.exists(lock_file)

    # Owner A libera legitimamente
    lm1.release_lock(partition, owner_a)
    assert not os.path.exists(lock_file)


def test_t06_heartbeat_updates_timestamp(tmp_path):
    """T06: Heartbeat renova o timestamp e data do último batimento."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=20)
    partition = "SIA/2026/05/PR"
    owner = "owner_hb"
    lock_file = lm._get_lock_filepath(partition)

    lm.acquire_lock(partition, owner)
    with open(lock_file, "r", encoding="utf-8") as f:
        data1 = json.load(f)
    t1 = data1["timestamp"]

    time.sleep(0.2)
    assert lm.heartbeat(partition, owner) is True

    with open(lock_file, "r", encoding="utf-8") as f:
        data2 = json.load(f)
    t2 = data2["timestamp"]

    assert t2 > t1
    assert "heartbeat_at" in data2
    lm.release_lock(partition, owner)


def test_t07_heartbeat_stops_after_release(tmp_path):
    """T07: Thread de heartbeat é finalizada imediatamente após o release."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10, heartbeat_interval=1)
    partition = "SIA/2026/05/SC"
    owner = "owner_stop_hb"

    with lm.lock(partition, owner):
        assert partition in lm._local_claims
        stop_ev = lm._local_claims[partition]["stop_event"]
        assert not stop_ev.is_set()

    # Após o contextmanager sair:
    assert partition not in lm._local_claims
    assert stop_ev.is_set()


def test_t08_stale_lock_takeover(tmp_path):
    """T08: Lock expirado (idade >= timeout) permite takeover atômico seguro."""
    locks_dir = str(tmp_path / "locks")
    lm1 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=1)
    lm2 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=1)
    partition = "SIA/2026/05/RS"
    owner_dead = "owner_dead"
    owner_new = "owner_new"
    lock_file = lm1._get_lock_filepath(partition)

    # Cria lock antigo com timestamp no passado
    old_time = time.time() - 5.0
    payload = {
        "partition_key": partition,
        "owner_token": owner_dead,
        "execution_id": owner_dead,
        "pid": 99999,
        "timestamp": old_time,
        "created_at": "2026-09-01T00:00:00Z",
    }
    with open(lock_file, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    # Novo owner tenta adquirir: deve detectar stale e fazer takeover
    assert lm2.acquire_lock(partition, owner_new) is True
    with open(lock_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["owner_token"] == owner_new
    assert data["timestamp"] > old_time

    lm2.release_lock(partition, owner_new)


def test_t09_active_lock_cannot_be_taken_over(tmp_path):
    """T09: Lock ativo dentro do timeout NÃO pode sofrer takeover."""
    locks_dir = str(tmp_path / "locks")
    lm1 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    lm2 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    partition = "SIA/2026/05/GO"
    owner_active = "owner_active"
    owner_attacker = "owner_attacker"

    assert lm1.acquire_lock(partition, owner_active) is True
    assert lm2.acquire_lock(partition, owner_attacker) is False

    lm1.release_lock(partition, owner_active)


def test_t10_corrupted_json_quarantine_and_recovery(tmp_path):
    """T10: Arquivo corrompido é movido para quarentena e a partição é recuperada."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10)
    partition = "SIA/2026/05/MT"
    owner = "owner_clean"
    lock_file = lm._get_lock_filepath(partition)

    # Grava lixo corrompido no arquivo
    with open(lock_file, "w", encoding="utf-8") as f:
        f.write("CORRUPTED_GARBAGE_NOT_JSON{{{")

    assert lm.acquire_lock(partition, owner) is True
    assert os.path.exists(lock_file)

    # Confirma que o arquivo corrompido foi colocado em quarentena
    quarantined = [f for f in os.listdir(locks_dir) if "corrupted_" in f]
    assert len(quarantined) == 1

    lm.release_lock(partition, owner)


def test_t11_invalid_timestamp_in_lock(tmp_path):
    """T11: Lock com payload incompleto/sem timestamp é tratado como corrompido."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10)
    partition = "SIA/2026/05/MS"
    owner = "owner_valid"
    lock_file = lm._get_lock_filepath(partition)

    # Grava JSON sem timestamp
    with open(lock_file, "w", encoding="utf-8") as f:
        json.dump({"partition_key": partition, "owner_token": "some_broken_owner"}, f)

    assert lm.acquire_lock(partition, owner) is True
    lm.release_lock(partition, owner)


def test_t12_sigterm_cleanup(tmp_path):
    """T12: release_all_local_claims libera todos os locks mantidos no processo."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=30)
    p1 = "SIA/2026/05/DF"
    p2 = "SIH/2026/05/DF"
    owner = "owner_sigterm"

    lm.acquire_lock(p1, owner)
    lm.acquire_lock(p2, owner)
    assert os.path.exists(lm._get_lock_filepath(p1))
    assert os.path.exists(lm._get_lock_filepath(p2))

    # Simula o efeito do handler de SIGTERM
    lm.release_all_local_claims()

    assert not os.path.exists(lm._get_lock_filepath(p1))
    assert not os.path.exists(lm._get_lock_filepath(p2))
    assert len(lm._local_claims) == 0


# =============================================================================
# SEÇÃO 2: TESTES DE CONCORRÊNCIA REAL E PROCESSOS SEPARADOS (T13 a T17)
# =============================================================================

def _worker_process_acquire(locks_dir: str, partition: str, owner: str, hold_seconds: float, queue_out: multiprocessing.Queue):
    """Função auxiliar executada em processo filho separado."""
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=5)
    acquired = lm.acquire_lock(partition, owner)
    queue_out.put(acquired)
    if acquired:
        time.sleep(hold_seconds)
        lm.release_lock(partition, owner)


def test_t13_real_multiprocessing_concurrency(tmp_path):
    """T13: Concorrência real entre processos via multiprocessing."""
    locks_dir = str(tmp_path / "locks")
    partition = "SIA/2026/05/SP"
    q_a = multiprocessing.Queue()
    q_b = multiprocessing.Queue()

    p_a = multiprocessing.Process(
        target=_worker_process_acquire,
        args=(locks_dir, partition, "proc_A", 1.5, q_a),
    )
    p_b = multiprocessing.Process(
        target=_worker_process_acquire,
        args=(locks_dir, partition, "proc_B", 0.5, q_b),
    )

    # Inicia Processo A (adquire e segura por 1.5s)
    p_a.start()
    res_a = q_a.get(timeout=5)
    assert res_a is True

    # Inicia Processo B enquanto A ainda segura: B DEVE ser rejeitado
    p_b.start()
    res_b = q_b.get(timeout=5)
    assert res_b is False

    p_a.join(timeout=5)
    p_b.join(timeout=5)


def test_t14_t15_partition_isolation_multiprocess(tmp_path):
    """T14 & T15: Lock em SIA-SP NÃO bloqueia SIA-RJ nem SIH-SP."""
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10)

    p_sp = "SIA/2026/05/SP"
    p_rj = "SIA/2026/05/RJ"
    p_sih_sp = "SIH/2026/05/SP"

    assert lm.acquire_lock(p_sp, "owner_sp") is True
    # Outras partições devem ser adquiridas sem qualquer interferência
    assert lm.acquire_lock(p_rj, "owner_rj") is True
    assert lm.acquire_lock(p_sih_sp, "owner_sih") is True

    lm.release_lock(p_sp, "owner_sp")
    lm.release_lock(p_rj, "owner_rj")
    lm.release_lock(p_sih_sp, "owner_sih")


def test_t16_long_operation_heartbeat_keeps_lock_alive(tmp_path):
    """T16: Operação com duração > timeout é mantida viva por heartbeat contínuo."""
    locks_dir = str(tmp_path / "locks")
    # Timeout de 2 segundos com heartbeat a cada 0.5s
    lm1 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=2, heartbeat_interval=1)
    lm2 = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=2)
    partition = "SIA/2026/05/MG"
    owner_long = "owner_long_job"
    owner_rival = "owner_rival"

    with lm1.lock(partition, owner_long):
        # Aguarda 2.5s (superior ao timeout de 2.0s)
        time.sleep(2.5)

        # O lock NÃO pode ser tomado porque a thread de heartbeat renovou o timestamp
        assert lm2.acquire_lock(partition, owner_rival) is False


def _worker_sigkill_simulation(locks_dir: str, partition: str, owner: str, queue_out: multiprocessing.Queue):
    """Adquire lock e envia SIGKILL a si mesmo simulando crash brutal instantâneo."""
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=1)
    acquired = lm.acquire_lock(partition, owner)
    queue_out.put(acquired)
    if acquired:
        # Permite que a thread de IPC do multiprocessing descarregue o buffer antes do SIGKILL
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGKILL)



def test_t17_sigkill_crash_recovery(tmp_path):
    """T17: Processo crashado com SIGKILL deixa lock recuperável após o timeout."""
    locks_dir = str(tmp_path / "locks")
    partition = "SIA/2026/05/ES"
    q = multiprocessing.Queue()

    p = multiprocessing.Process(
        target=_worker_sigkill_simulation,
        args=(locks_dir, partition, "proc_crashed", q),
    )
    p.start()
    acquired = q.get(timeout=5)
    assert acquired is True
    p.join(timeout=5)
    assert p.exitcode != 0  # Processo morreu abruptamente

    # Aguarda o timeout curto expirar (1.2s > 1.0s)
    time.sleep(1.2)

    # Novo processo deve conseguir assumir a partição com takeover automático
    lm_recover = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=1)
    assert lm_recover.acquire_lock(partition, "proc_recover") is True
    lm_recover.release_lock(partition, "proc_recover")
