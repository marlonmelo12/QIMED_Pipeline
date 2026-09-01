"""
Testes de Integracao e Governanca:
1. Heartbeat do Lock durante streaming longo.
2. Predicate Builder com Introspeccao de Schema (tipos string, int e misto).
3. Isolamento de particao e atomicidade no overwrite Delta Lake (SP intocado no retry de MG).
4. Execucao de ponta a ponta em master_pipeline.py comprovando que AdaptiveRetryExhausted
   nao derruba o pipeline, libera o lock no disco e grava o manifesto com status="failed".
5. Reducao efetiva de batch com piso degradado (16MB + min_batch_rows_degraded).
"""
import os
import time
import shutil
import hashlib
import tempfile
import pytest
from unittest.mock import MagicMock, patch
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.ingestion.lock_manager import PartitionLockManager
from src.ingestion.batch_sink import calculate_dynamic_batch_size, DeltaBatchSink
from src.ingestion.predicate_builder import build_partition_predicate
from src.observability.memory_governor import MemoryPressureError
from src.pipeline.master_pipeline import QimedMasterPipeline
from src.ingestion.manifest_delta import DeltaManifestManager



# -----------------------------------------------------------------------------
# ITEM 1: Heartbeat do Lock durante processamento longo
# -----------------------------------------------------------------------------
def test_lock_heartbeat_extends_ttl(tmp_path):
    """
    Comprova que o metodo heartbeat() renova o timestamp do lock
    e impede que outra execucao adquira a particao durante processos longos.
    """
    locks_dir = str(tmp_path / "locks")
    lock_mgr = PartitionLockManager(locks_dir=locks_dir, timeout_seconds=2)
    partition = "SIA/2026/05/MG"
    exec_1 = "exec_worker_1"
    exec_2 = "exec_worker_2"

    # Worker 1 adquire o lock
    assert lock_mgr.acquire_lock(partition, exec_1) is True

    # Worker 2 tenta adquirir e falha
    assert lock_mgr.acquire_lock(partition, exec_2) is False

    # Simula passagem de tempo (1.2s) e heartbeat ativo do Worker 1
    time.sleep(1.2)
    assert lock_mgr.heartbeat(partition, exec_1) is True

    # Passa mais 1.0s (tempo total decorrido = 2.2s > timeout original de 2.0s)
    time.sleep(1.0)

    # Worker 2 tenta adquirir: DEVE FALHAR porque o Worker 1 renovou o lock
    assert lock_mgr.acquire_lock(partition, exec_2) is False

    # Worker 1 libera o lock
    lock_mgr.release_lock(partition, exec_1)

    # Agora Worker 2 consegue adquirir
    assert lock_mgr.acquire_lock(partition, exec_2) is True
    lock_mgr.release_lock(partition, exec_2)


# -----------------------------------------------------------------------------
# ITEM 2: Predicate Builder Type-Safe (String, Int e Misto)
# -----------------------------------------------------------------------------
def test_build_partition_predicate_string_types():
    """Schema com ano/mes como string gera predicado com aspas."""
    schema = pa.schema([
        pa.field("ano", pa.string()),
        pa.field("mes", pa.string()),
        pa.field("uf", pa.string()),
    ])
    predicate = build_partition_predicate(schema, year=2026, month=5, uf="MG")
    assert predicate == "ano = '2026' AND mes = '05' AND uf = 'MG'"


def test_build_partition_predicate_integer_types():
    """Schema com ano/mes como inteiros gera predicado numerico sem aspas."""
    schema = pa.schema([
        pa.field("ano", pa.int32()),
        pa.field("mes", pa.int64()),
        pa.field("uf", pa.string()),
    ])
    predicate = build_partition_predicate(schema, year=2026, month=5, uf="MG")
    assert predicate == "ano = 2026 AND mes = 5 AND uf = 'MG'"


def test_build_partition_predicate_mixed_types():
    """Schema misto (ano string, mes int) gera clausulas respeitando cada tipo."""
    schema = pa.schema([
        pa.field("ano", pa.string()),
        pa.field("mes", pa.int32()),
        pa.field("uf", pa.string()),
    ])
    predicate = build_partition_predicate(schema, year=2026, month=5, uf="MG")
    assert predicate == "ano = '2026' AND mes = 5 AND uf = 'MG'"


# -----------------------------------------------------------------------------
# ITEM 3: Isolamento de Particao no Overwrite com Predicate Dinamico
# -----------------------------------------------------------------------------
def test_delta_partition_isolation_on_retry(tmp_path):
    """
    Comprova que um overwrite com predicate dinamico em MG
    mantem os dados de SP 100% intocados (mesma contagem, tipos e valores).
    """
    delta_dir = str(tmp_path / "bronze_delta")
    os.makedirs(delta_dir, exist_ok=True)

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("valor", pa.float64()),
        pa.field("ano", pa.string()),
        pa.field("mes", pa.string()),
        pa.field("uf", pa.string()),
    ])

    sp_table = pa.Table.from_arrays([
        pa.array(list(range(100)), pa.int64()),
        pa.array([10.5 * i for i in range(100)], pa.float64()),
        pa.array(["2026"] * 100, pa.string()),
        pa.array(["05"] * 100, pa.string()),
        pa.array(["SP"] * 100, pa.string()),
    ], schema=schema)

    mg_table_v1 = pa.Table.from_arrays([
        pa.array(list(range(100)), pa.int64()),
        pa.array([20.0 * i for i in range(100)], pa.float64()),
        pa.array(["2026"] * 100, pa.string()),
        pa.array(["05"] * 100, pa.string()),
        pa.array(["MG"] * 100, pa.string()),
    ], schema=schema)

    write_deltalake(delta_dir, sp_table, mode="append", partition_by=["ano", "mes", "uf"])
    write_deltalake(delta_dir, mg_table_v1, mode="append", partition_by=["ano", "mes", "uf"])

    dt_before = DeltaTable(delta_dir)
    sp_df_before = dt_before.to_pandas().query("uf == 'SP'").sort_values("id").reset_index(drop=True)
    assert len(sp_df_before) == 100

    # Retry de MG com 150 linhas usando build_partition_predicate
    mg_table_retry = pa.Table.from_arrays([
        pa.array(list(range(150)), pa.int64()),
        pa.array([30.0 * i for i in range(150)], pa.float64()),
        pa.array(["2026"] * 150, pa.string()),
        pa.array(["05"] * 150, pa.string()),
        pa.array(["MG"] * 150, pa.string()),
    ], schema=schema)

    predicate = build_partition_predicate(mg_table_retry.schema, year=2026, month=5, uf="MG")
    assert predicate == "ano = '2026' AND mes = '05' AND uf = 'MG'"

    write_deltalake(
        delta_dir,
        mg_table_retry,
        mode="overwrite",
        predicate=predicate,
        partition_by=["ano", "mes", "uf"],
    )

    dt_after = DeltaTable(delta_dir)
    full_df = dt_after.to_pandas()

    sp_df_after = full_df.query("uf == 'SP'").sort_values("id").reset_index(drop=True)
    mg_df_after = full_df.query("uf == 'MG'").sort_values("id").reset_index(drop=True)

    import pandas.testing as pdt
    assert len(sp_df_after) == 100
    pdt.assert_frame_equal(sp_df_before, sp_df_after)
    assert len(mg_df_after) == 150


# -----------------------------------------------------------------------------
# ITEM 4: Pipeline de Producao Completo com Dupla Falha e Liberacao de Lock
# -----------------------------------------------------------------------------
def test_pipeline_continues_on_exhausted_retry_and_releases_lock(tmp_path):
    """
    Exercita o loop REAL do execute_bronze_ingestion() do master_pipeline.py:
    1. Simula falha dupla (MemoryPressureError) em MG e sucesso em SP;
    2. Valida que o pipeline NAO quebra e processa SP normalmente;
    3. Valida que o manifesto grava status='failed' para MG com 'AdaptiveRetryExhausted';
    4. Valida que o arquivo de lock de MG e LIBERADO do disco.
    """
    system_dir = str(tmp_path / "system")
    locks_dir = str(tmp_path / "system" / "locks")
    manifest_dir = str(tmp_path / "system" / "manifest")
    bronze_dir = str(tmp_path / "bronze")
    os.makedirs(locks_dir, exist_ok=True)
    os.makedirs(manifest_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)

    cfg = {
        "paths": {"system_dir": system_dir, "bronze_dir": bronze_dir},
        "duckdb": {"memory_limit": "8GB"},
        "staging": {"enrichment_batch_target_mb": 64},
        "resources": {"min_batch_rows_degraded": 2000},
    }


    pipe = QimedMasterPipeline()
    pipe.cfg = cfg
    pipe.lock_manager = PartitionLockManager(locks_dir=locks_dir)
    pipe.manifest_manager = DeltaManifestManager(system_dir=system_dir)

    def mock_commit(self, stg, execution_id, source_files=None):
        if self.uf == "MG":
            raise MemoryPressureError("Simulated OOM for MG in both attempts")
        return {"status": "success", "total_rows": 500}

    with patch("src.collectors.datasus_collector.DatasusCollector.fetch", return_value="fake.dbc"), \
         patch("src.collectors.datasus_collector.DatasusCollector.parse_record_batches", return_value=iter([])), \
         patch("src.ingestion.staging_writer.ParquetStagingWriter.process_batch_stream", return_value=None), \
         patch("src.ingestion.batch_sink.DeltaBatchSink.commit_staging_to_bronze", side_effect=mock_commit, autospec=True):

        # Executa o loop REAL de producao apenas para MG e SP
        result = pipe.execute_bronze_ingestion(
            target_month=5,
            target_year=2026,
            force_reprocess=True,
            uf_list=["MG", "SP"],
        )

    # 1. Assert: O pipeline concluiu e registrou a falha de MG sem crashar
    failed_partitions = [f["partition"] for f in result["failed_partitions"]]
    assert "SIH/2026/05/MG" in failed_partitions or "SIA/2026/05/MG" in failed_partitions

    # 2. Assert: O manifesto registrou status="failed" com mensagem de AdaptiveRetryExhausted
    manifest_df = DeltaTable(pipe.manifest_manager.manifest_table_path).to_pandas()
    mg_failed = manifest_df.query("uf == 'MG' and status == 'failed'")
    assert len(mg_failed) > 0
    assert "AdaptiveRetryExhausted" in mg_failed.iloc[0]["mensagem_erro"]

    # 3. Assert: O lock de MG foi LIBERADO no disco (arquivo não existe)
    lock_file_sih_mg = pipe.lock_manager._get_lock_filepath("SIH/2026/05/MG")
    assert not os.path.exists(lock_file_sih_mg)



# -----------------------------------------------------------------------------
# ITEM 5: Reducao Efetiva de Batch com Piso Degradado (Tabelas Largas / MG)
# -----------------------------------------------------------------------------
def test_degraded_floor_allows_batch_size_reduction():
    """
    Comprova numericamente que o piso degradado (min_rows=2000) permite que o
    orcamento de 16MB reduza efetivamente o batch_size em tabelas largas de 2KB/linha.
    """
    class WideTableMeta:
        num_rows = 1_000_000
        serialized_size = 2_000_000_000  # 2.000 bytes/linha

    meta = WideTableMeta()

    # Tentativa 1: Orçamento Normal 64MB com piso normal (10.000)
    batch_size_orig = calculate_dynamic_batch_size(meta, target_batch_mb=64.0, min_rows=10_000, max_rows=250_000)
    assert 33_000 <= batch_size_orig <= 34_000

    # Tentativa 2 COM PISO ANTIGO (10.000): 16MB (16.777.216) / 2000 = 8.388 -> clampava em 10.000
    batch_clamped = calculate_dynamic_batch_size(meta, target_batch_mb=16.0, min_rows=10_000, max_rows=250_000)
    assert batch_clamped == 10_000

    # Tentativa 2 COM PISO DEGRADADO (2.000): permite descer ate 8.388 linhas (reducao real de 75% da carga!)
    batch_degraded = calculate_dynamic_batch_size(meta, target_batch_mb=16.0, min_rows=2_000, max_rows=250_000)
    assert 8_300 <= batch_degraded <= 8_500
    assert batch_degraded < batch_clamped
