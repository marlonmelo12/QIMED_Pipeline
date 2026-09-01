"""
Testes Obrigatorios de Hardening de Memoria, Estabilidade e Resiliencia - QIMED Lakehouse V3.
Cobre integralmente os 13 cenarios especificados no Prompt Mestre:
1. Parquet pequeno
2. Parquet com muitas colunas (61 colunas)
3. Parquet com strings grandes
4. Parquet com muitos nulls
5. Parquet com multiplos row groups
6. Erro OSError(errno=12)
7. Erro de memoria encapsulado em DeltaError
8. DeltaError nao relacionado a memoria (prova que NAO dispara retry)
9. Retry adaptativo com sucesso na 2a tentativa
10. Exceder MAX_RETRIES (falha controlada com AdaptiveRetryExhausted)
11. Multiplos arquivos sequenciais com medicao de estabilidade de RSS
12. Dataset representativo do SIA-MA
13. Dataset representativo do SIA-MG
"""
import os
import gc
import psutil
import pytest
import tempfile
from unittest.mock import patch, MagicMock
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from src.observability.memory_governor import (
    calculate_dynamic_batch_size,
    is_memory_pressure_error,
    MemoryGovernor,
    MemoryPressureError,
)
from src.ingestion.batch_sink import DeltaBatchSink
from src.pipeline.master_pipeline import QimedMasterPipeline
from src.ingestion.lock_manager import PartitionLockManager
from src.ingestion.manifest_delta import DeltaManifestManager


# -----------------------------------------------------------------------------
# Teste 1: Parquet pequeno
# -----------------------------------------------------------------------------
def test_1_small_parquet(tmp_path):
    table = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    fpath = str(tmp_path / "small.parquet")
    pq.write_table(table, fpath)

    pf = pq.ParquetFile(fpath)
    batch_size = calculate_dynamic_batch_size(pf.metadata, target_batch_mb=64.0, min_rows=10, max_rows=1000)
    assert batch_size >= 10
    batches = list(pf.iter_batches(batch_size=batch_size))
    assert sum(len(b) for b in batches) == 3


# -----------------------------------------------------------------------------
# Teste 2: Parquet com muitas colunas (61 colunas, similar a SIA-MG)
# -----------------------------------------------------------------------------
def test_2_wide_parquet_61_columns(tmp_path):
    data = {f"col_{i}": ["sample_val_1234567890"] * 100 for i in range(61)}
    table = pa.table(data)
    fpath = str(tmp_path / "wide_61.parquet")
    pq.write_table(table, fpath)

    pf = pq.ParquetFile(fpath)
    # Com 61 colunas, o custo por linha eh alto e o batch_size deve ser contido
    batch_size = calculate_dynamic_batch_size(pf.metadata, target_batch_mb=64.0, min_rows=1000, max_rows=250000)
    assert batch_size < 50000  # Deve ser bem menor que 250k


# -----------------------------------------------------------------------------
# Teste 3: Parquet com strings grandes
# -----------------------------------------------------------------------------
def test_3_large_strings_parquet(tmp_path):
    # Strings com entropia para evitar compressao Snappy artifical de repeticao pura
    data = {
        "id": list(range(100)),
        "text_payload": [f"user_{i}_log_data_" + ("abcdefghijklmnopqrstuvwxyz0123456789" * 60) for i in range(100)],
    }
    table = pa.table(data)
    fpath = str(tmp_path / "large_strings.parquet")
    pq.write_table(table, fpath)

    pf = pq.ParquetFile(fpath)
    batch_size = calculate_dynamic_batch_size(pf.metadata, target_batch_mb=64.0, min_rows=100, max_rows=250000)
    assert batch_size <= 250000
    batches = list(pf.iter_batches(batch_size=batch_size))
    assert sum(len(b) for b in batches) == 100


# -----------------------------------------------------------------------------
# Teste 4: Parquet com muitos nulls (80% nulos)
# -----------------------------------------------------------------------------
def test_4_high_null_ratio_parquet(tmp_path):
    data = {"id": list(range(100)), "sparse_val": [None if i % 5 != 0 else "valid" for i in range(100)]}
    table = pa.table(data)
    fpath = str(tmp_path / "sparse.parquet")
    pq.write_table(table, fpath)

    pf = pq.ParquetFile(fpath)
    batch_size = calculate_dynamic_batch_size(pf.metadata, target_batch_mb=64.0, min_rows=10, max_rows=1000)
    assert batch_size >= 10
    batches = list(pf.iter_batches(batch_size=batch_size))
    assert sum(len(b) for b in batches) == 100


# -----------------------------------------------------------------------------
# Teste 5: Parquet com multiplos row groups
# -----------------------------------------------------------------------------
def test_5_multiple_row_groups(tmp_path):
    table = pa.table({"id": list(range(1000)), "val": ["item"] * 1000})
    fpath = str(tmp_path / "multi_rg.parquet")
    with pq.ParquetWriter(fpath, table.schema) as writer:
        for i in range(5):
            writer.write_table(table.slice(i * 200, 200))

    pf = pq.ParquetFile(fpath)
    assert pf.metadata.num_row_groups == 5
    batch_size = calculate_dynamic_batch_size(pf.metadata, target_batch_mb=64.0, min_rows=50, max_rows=500)
    batches = list(pf.iter_batches(batch_size=batch_size))
    assert sum(len(b) for b in batches) == 1000


# -----------------------------------------------------------------------------
# Teste 6: Erro OSError(errno=12) classificado como Memory Pressure
# -----------------------------------------------------------------------------
def test_6_oserror_errno_12_is_memory_pressure():
    err = OSError(12, "Cannot allocate memory")
    assert is_memory_pressure_error(err) is True


# -----------------------------------------------------------------------------
# Teste 7: Erro de memoria encapsulado em DeltaError
# -----------------------------------------------------------------------------
def test_7_delta_error_with_oom_is_memory_pressure():
    class DeltaError(Exception):
        pass

    inner_err = OSError(12, "Cannot allocate memory")
    delta_err = DeltaError("Generic DeltaTable error: Arrow error: IOError: [Errno 12] Cannot allocate memory")
    delta_err.__cause__ = inner_err

    assert is_memory_pressure_error(delta_err) is True


# -----------------------------------------------------------------------------
# Teste 8: DeltaError NAO relacionado a memoria (OBRIGATORIO: NAO dispara retry)
# -----------------------------------------------------------------------------
def test_8_delta_error_non_oom_does_not_trigger_memory_retry():
    class DeltaError(Exception):
        pass

    # Casos de erro que NAO sao falta de memoria
    schema_err = DeltaError("Schema mismatch: Column pa_cns_pac is missing in target Delta table")
    auth_err = DeltaError("S3 Access Denied: 403 Forbidden on bucket qimed-lakehouse")
    conflict_err = DeltaError("Transaction commit conflict: Version 42 already exists")

    assert is_memory_pressure_error(schema_err) is False
    assert is_memory_pressure_error(auth_err) is False
    assert is_memory_pressure_error(conflict_err) is False

    # Valida no master_pipeline que DeltaError generico eh re-lancado sem retry
    pipe = QimedMasterPipeline()
    with patch("src.ingestion.batch_sink.DeltaBatchSink.commit_staging_to_bronze", side_effect=schema_err):
        with pytest.raises(DeltaError):
            pipe._commit_with_adaptive_retry("sih", 2026, 5, "MG", None, "exec_123", [])


# -----------------------------------------------------------------------------
# Teste 9: Retry adaptativo com sucesso na 2a tentativa
# -----------------------------------------------------------------------------
def test_9_adaptive_retry_success_on_second_attempt():
    pipe = QimedMasterPipeline()
    attempts = []

    def mock_commit(self, stg, execution_id, source_files=None):
        target_mb = self.cfg.get("staging", {}).get("enrichment_batch_target_mb", 64.0)
        attempts.append(target_mb)
        if len(attempts) == 1:
            raise OSError(12, "Cannot allocate memory")
        return {"status": "success", "total_rows": 500}

    with patch("src.ingestion.batch_sink.DeltaBatchSink.commit_staging_to_bronze", side_effect=mock_commit, autospec=True):
        res = pipe._commit_with_adaptive_retry("sia", 2026, 5, "MG", None, "exec_retry_test", [])
        assert res["status"] == "success"
        assert attempts == [64.0, 16.0]  # Provou que a 2a tentativa degradou para 16MB


# -----------------------------------------------------------------------------
# Teste 10: Exceder MAX_RETRIES (falha controlada com AdaptiveRetryExhausted)
# -----------------------------------------------------------------------------
def test_10_exceed_max_retries_raises_adaptive_retry_exhausted():
    pipe = QimedMasterPipeline()

    def mock_commit_always_oom(self, stg, execution_id, source_files=None):
        raise OSError(12, "Cannot allocate memory in both attempts")

    with patch("src.ingestion.batch_sink.DeltaBatchSink.commit_staging_to_bronze", side_effect=mock_commit_always_oom, autospec=True):
        with pytest.raises(RuntimeError, match="AdaptiveRetryExhausted"):
            pipe._commit_with_adaptive_retry("sia", 2026, 5, "MG", None, "exec_fail_test", [])


# -----------------------------------------------------------------------------
# Teste 11: Multiplos arquivos sequenciais e estabilidade de RSS
# -----------------------------------------------------------------------------
def test_11_multiple_files_sequential_rss_stability(tmp_path):
    files = []
    for i in range(10):
        t = pa.table({"col_a": list(range(5000)), "col_b": ["val_" + str(x) for x in range(5000)]})
        p = str(tmp_path / f"part_{i:02d}.parquet")
        pq.write_table(t, p)
        files.append(p)

    proc = psutil.Process(os.getpid())
    rss_initial = proc.memory_info().rss / (1024 * 1024)

    for f in files:
        pf = pq.ParquetFile(f)
        for b in pf.iter_batches(batch_size=1000):
            _ = len(b)
            del b
        del pf
        gc.collect()
        pa.default_memory_pool().release_unused()

    rss_final = proc.memory_info().rss / (1024 * 1024)
    # A variacao de RSS apos 10 arquivos deve ser minima (< 50MB)
    assert abs(rss_final - rss_initial) < 50.0


# -----------------------------------------------------------------------------
# Teste 12: Dataset representativo do SIA-MA (15 colunas, streaming Delta)
# -----------------------------------------------------------------------------
def test_12_representative_sia_ma_dataset(tmp_path):
    stg_dir = str(tmp_path / "staging")
    delta_dir = str(tmp_path / "bronze_ma")
    os.makedirs(stg_dir, exist_ok=True)

    # 15 colunas incluindo as obrigatorias de validacao do SIA
    cols = {
        "PA_CODUNI": ["1234567"] * 2000,
        "PA_PROC_ID": ["0301010072"] * 2000,
        "PA_QTDPRO": ["1"] * 2000,
        "PA_VALPRO": ["150.50"] * 2000,
    }
    for i in range(11):
        cols[f"pa_col_{i}"] = [f"data_{j}" for j in range(2000)]

    t = pa.table(cols)
    fpath = os.path.join(stg_dir, "compacted-part-0001.parquet")
    pq.write_table(t, fpath)

    cfg = {
        "paths": {"staging_dir": stg_dir},
        "staging": {"enrichment_batch_target_mb": 64.0},
        "resources": {"min_batch_rows": 500, "max_batch_rows": 10000},
    }
    sink = DeltaBatchSink(subsystem="sia", year=2026, month=5, uf="MA", config=cfg)
    sink.target_delta_table = delta_dir

    mock_stg = MagicMock()
    mock_stg.get_staged_files.return_value = [fpath]
    mock_stg.cleanup_staging.return_value = None

    res = sink.commit_staging_to_bronze(mock_stg, execution_id="exec_ma_test", source_files=[fpath])
    assert res["status"] in ("committed", "success")
    assert res["total_rows"] == 2000

    dt = DeltaTable(delta_dir)
    assert dt.to_pyarrow_table().num_rows == 2000


# -----------------------------------------------------------------------------
# Teste 13: Dataset representativo do SIA-MG (61 colunas, memory bounded)
# -----------------------------------------------------------------------------
def test_13_representative_sia_mg_dataset(tmp_path):
    stg_dir = str(tmp_path / "staging_mg")
    delta_dir = str(tmp_path / "bronze_mg")
    os.makedirs(stg_dir, exist_ok=True)

    # 61 colunas realistas incluindo as 4 obrigatorias do SIA
    cols = {
        "PA_CODUNI": ["7654321"] * 3000,
        "PA_PROC_ID": ["0301010072"] * 3000,
        "PA_QTDPRO": ["1"] * 3000,
        "PA_VALPRO": ["250.75"] * 3000,
    }
    for i in range(57):
        cols[f"col_mg_{i}"] = [f"valor_longo_{j}_exemplo_saude" for j in range(3000)]

    t = pa.table(cols)
    fpath = os.path.join(stg_dir, "compacted-part-0001.parquet")
    pq.write_table(t, fpath)

    cfg = {
        "paths": {"staging_dir": stg_dir},
        "staging": {"enrichment_batch_target_mb": 64.0},
        "resources": {"min_batch_rows": 500, "max_batch_rows": 10000},
    }
    sink = DeltaBatchSink(subsystem="sia", year=2026, month=5, uf="MG", config=cfg)
    sink.target_delta_table = delta_dir

    mock_stg = MagicMock()
    mock_stg.get_staged_files.return_value = [fpath]
    mock_stg.cleanup_staging.return_value = None

    res = sink.commit_staging_to_bronze(mock_stg, execution_id="exec_mg_test", source_files=[fpath])
    assert res["status"] in ("committed", "success")
    assert res["total_rows"] == 3000

    dt = DeltaTable(delta_dir)
    assert dt.to_pyarrow_table().num_rows == 3000


