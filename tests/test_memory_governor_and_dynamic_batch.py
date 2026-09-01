"""
Testes Unitários para Dynamic Batching, Memory Governor e Liberação do Pool Arrow.
"""
import os
import pytest
from unittest.mock import MagicMock, patch
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.batch_sink import calculate_dynamic_batch_size, DeltaBatchSink
from src.observability.memory_governor import (
    MemoryGovernor,
    MemoryPressureError,
    parse_memory_limit_to_mb,
    get_cgroup_memory_limit_mb,
)


class MockParquetMetadata:
    def __init__(self, num_rows: int, serialized_size: int):
        self.num_rows = num_rows
        self.serialized_size = serialized_size


def test_parse_memory_limit_to_mb():
    """Testa a conversao precisa de strings e numeros para Megabytes."""
    assert parse_memory_limit_to_mb("8GB") == 8192.0
    assert parse_memory_limit_to_mb("4GiB") == 4096.0
    assert parse_memory_limit_to_mb("512MB") == 512.0
    assert parse_memory_limit_to_mb("1024M") == 1024.0
    assert parse_memory_limit_to_mb(2048) == 2048.0
    assert parse_memory_limit_to_mb(None) == 8192.0
    assert parse_memory_limit_to_mb("invalid") == 8192.0


def test_calculate_dynamic_batch_size_wide_table():
    """Tabela larga (ex: DATASUS 61 colunas, ~100 bytes/linha comprimido)."""
    # 10M linhas, 1GB comprimido (~100 bytes/linha)
    meta = MockParquetMetadata(num_rows=10_000_000, serialized_size=1_000_000_000)
    # Alvo 64MB = ~67.1MB -> 67_108_864 / 100 = 671_088 linhas -> clampar no max_rows (250_000)
    batch_size = calculate_dynamic_batch_size(meta, target_batch_mb=64.0, min_rows=10_000, max_rows=250_000)
    assert batch_size == 250_000


def test_calculate_dynamic_batch_size_heavy_wide_table():
    """Tabela extremamente pesada (ex: 2000 bytes/linha)."""
    meta = MockParquetMetadata(num_rows=1_000_000, serialized_size=2_000_000_000)  # 2KB/linha
    # 64MB (67_108_864 bytes) / 2000 bytes = 33_554 linhas
    batch_size = calculate_dynamic_batch_size(meta, target_batch_mb=64.0, min_rows=10_000, max_rows=250_000)
    assert 33_000 <= batch_size <= 34_000


def test_calculate_dynamic_batch_size_clamps_to_floor():
    """Tabela massiva por linha (ex: 10KB/linha) forçando o piso de 10.000 linhas."""
    meta = MockParquetMetadata(num_rows=100_000, serialized_size=1_000_000_000)  # 10KB/linha
    # 64MB / 10KB = 6.400 linhas -> deve clampar no piso de 10.000
    batch_size = calculate_dynamic_batch_size(meta, target_batch_mb=64.0, min_rows=10_000, max_rows=250_000)
    assert batch_size == 10_000


def test_calculate_dynamic_batch_size_edge_cases():
    """Testa casos de borda como None, metadados vazios e dicionarios."""
    assert calculate_dynamic_batch_size(None) == 10_000
    assert calculate_dynamic_batch_size(MockParquetMetadata(num_rows=0, serialized_size=100)) == 10_000
    assert calculate_dynamic_batch_size(MockParquetMetadata(num_rows=100, serialized_size=0)) == 10_000

    # Dicionario mock
    dict_meta = {"num_rows": 500_000, "serialized_size": 50_000_000}
    batch_size = calculate_dynamic_batch_size(dict_meta, target_batch_mb=32.0)
    assert 10_000 <= batch_size <= 250_000


def test_memory_governor_derives_limit_from_config():
    """Valida que o soft limit e derivado da configuracao do DuckDB."""
    cfg = {"duckdb": {"memory_limit": "4GB"}, "resources": {"memory_pressure_threshold": 0.80}}
    gov = MemoryGovernor(config=cfg)
    assert gov.soft_limit_mb <= 4096.0
    assert gov.pressure_threshold == 0.80
    assert gov.threshold_mb == gov.soft_limit_mb * 0.80


def test_memory_governor_checkpoint_healthy():
    """Valida checkpoint com RSS saudável injetado deterministicamente."""
    cfg = {"duckdb": {"memory_limit": "8GB"}, "resources": {"memory_pressure_threshold": 0.85}}
    gov = MemoryGovernor(config=cfg, soft_limit_mb=8192.0)
    res = gov.checkpoint(context="SIA-MG", current_rss_mb=2048.0)
    assert res["status"] == "ok"
    assert res["rss_mb"] == 2048.0


def test_memory_governor_checkpoint_pressure_raises_custom_exception():
    """Valida que MemoryPressureError e levantado deterministicamente quando RSS excede 85%."""
    cfg = {"duckdb": {"memory_limit": "8GB"}, "resources": {"memory_pressure_threshold": 0.85}}
    gov = MemoryGovernor(config=cfg, soft_limit_mb=8192.0)
    # Threshold = 8192 * 0.85 = 6963.2 MB
    with pytest.raises(MemoryPressureError) as exc_info:
        gov.checkpoint(context="SIA/2026/05/MG (arquivo 7/8)", current_rss_mb=7200.0)

    assert "MemoryPressureError" in str(exc_info.value)
    assert "SIA/2026/05/MG" in str(exc_info.value)
    assert "7200.0MB" in str(exc_info.value)


def test_memory_governor_triggers_pool_release_on_pressure():
    """Valida que gc.collect e pa.default_memory_pool().release_unused() sao chamados na pressao."""
    gov = MemoryGovernor(soft_limit_mb=1000.0, pressure_threshold=0.5)
    with patch("pyarrow.default_memory_pool") as mock_pool, patch("gc.collect") as mock_gc:
        mock_pool_inst = MagicMock()
        mock_pool.return_value = mock_pool_inst

        with pytest.raises(MemoryPressureError):
            gov.checkpoint(context="Test", current_rss_mb=800.0)

        mock_gc.assert_called_once()
        mock_pool_inst.release_unused.assert_called_once()


def test_cgroup_v2_max_returns_none(tmp_path):
    """Cgroups v2 com 'max' (sem limite) deve retornar None."""
    cgroup_file = tmp_path / "memory.max"
    cgroup_file.write_text("max\n", encoding="utf-8")
    assert get_cgroup_memory_limit_mb(cgroup_v2_path=str(cgroup_file)) is None


def test_cgroup_v2_bounded_limit_returns_mb(tmp_path):
    """Cgroups v2 com valor numerico (ex: 4GB = 4294967296 bytes) deve retornar 4096.0 MB."""
    cgroup_file = tmp_path / "memory.max"
    cgroup_file.write_text("4294967296\n", encoding="utf-8")
    assert get_cgroup_memory_limit_mb(cgroup_v2_path=str(cgroup_file)) == 4096.0


def test_cgroup_v1_sentinel_returns_none(tmp_path):
    """Cgroups v1 com sentinel ilimitado (ex: 9223372036854771712 bytes) deve retornar None."""
    cgroup_file = tmp_path / "memory.limit_in_bytes"
    cgroup_file.write_text("9223372036854771712\n", encoding="utf-8")
    assert get_cgroup_memory_limit_mb(cgroup_v1_path=str(cgroup_file)) is None


def test_cgroup_v1_bounded_limit_returns_mb(tmp_path):
    """Cgroups v1 com valor numerico (ex: 2GB = 2147483648 bytes) deve retornar 2048.0 MB."""
    cgroup_file = tmp_path / "memory.limit_in_bytes"
    cgroup_file.write_text("2147483648\n", encoding="utf-8")
    assert get_cgroup_memory_limit_mb(cgroup_v1_path=str(cgroup_file)) == 2048.0


def test_cgroup_non_existent_file_returns_none():
    """Arquivos de cgroup inexistentes devem retornar None de forma segura."""
    assert get_cgroup_memory_limit_mb(cgroup_v2_path="/non/existent/path") is None


def test_memory_governor_ignores_unbounded_cgroup_fallback_to_yaml():
    """Valida que quando o cgroup retorna None (sem limite), o governor usa o limite do YAML/RAM."""
    cfg = {"duckdb": {"memory_limit": "6GB"}, "resources": {"memory_pressure_threshold": 0.85}}
    with patch("src.observability.memory_governor.get_cgroup_memory_limit_mb", return_value=None):
        gov = MemoryGovernor(config=cfg)
        # O soft limit deve ser min(6144.0, sys_ram_mb), nunca um numero gigante
        assert gov.soft_limit_mb <= 6144.0

