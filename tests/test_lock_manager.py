"""
Teste do PartitionLockManager - QIMED Lakehouse V3.
"""
import pytest
from src.ingestion.lock_manager import PartitionLockManager


def test_partition_lock(tmp_path):
    locks_dir = str(tmp_path / "locks")
    lm = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=10)

    partition_key = "SIA/2026/05/MG"
    exec_1 = "exec_001"
    exec_2 = "exec_002"

    # Adquire lock 1
    assert lm.acquire_lock(partition_key, exec_1) is True

    # Lock 2 concorrente deve falhar
    assert lm.acquire_lock(partition_key, exec_2) is False

    # Libera lock 1
    lm.release_lock(partition_key, exec_1)

    # Agora lock 2 consegue adquirir
    assert lm.acquire_lock(partition_key, exec_2) is True
    lm.release_lock(partition_key, exec_2)
