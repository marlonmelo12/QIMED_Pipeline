"""
Testes Automatizados — Bounded-Memory Compaction & Delta Batch Sink Streaming.
"""
import os
import shutil
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from src.lakehouse.compaction import DeltaCompactor
from src.ingestion.batch_sink import DeltaBatchSink
from src.ingestion.staging_writer import ParquetStagingWriter


@pytest.fixture
def temp_workspace():
    tmpdir = tempfile.mkdtemp(prefix="qimed_test_bounded_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_compaction_single_file_direct_reuse(temp_workspace):
    """Garante que com 1 arquivo o compactor reaproveita diretamente sem reescrever."""
    f1 = os.path.join(temp_workspace, "part-0001.parquet")
    tbl = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    pq.write_table(tbl, f1)

    compactor = DeltaCompactor()
    out_dir = os.path.join(temp_workspace, "compacted")
    res = compactor.compact_staging_files([f1], uf="AC", output_dir=out_dir)

    assert res == [f1]


def test_compaction_empty_list():
    """Garante que lista vazia retorna lista vazia sem erro."""
    compactor = DeltaCompactor()
    assert compactor.compact_staging_files([], uf="SP", output_dir="dummy") == []


def test_compaction_multiple_files_streaming(temp_workspace):
    """Garante que múltiplos arquivos de staging são compactados em streaming mantendo contagem de linhas e schema."""
    staging_files = []
    schema = pa.schema([("id", pa.int64()), ("valor", pa.float64()), ("nome", pa.string())])
    total_expected_rows = 15000

    for i in range(5):
        f = os.path.join(temp_workspace, f"staging_part_{i:03d}.parquet")
        rows = 3000
        tbl = pa.table({
            "id": pa.array(range(i * rows, (i + 1) * rows), pa.int64()),
            "valor": pa.array([float(x) for x in range(rows)], pa.float64()),
            "nome": pa.array([f"item_{x}" for x in range(rows)], pa.string()),
        }, schema=schema)
        pq.write_table(tbl, f)
        staging_files.append(f)

    compactor = DeltaCompactor()
    out_dir = os.path.join(temp_workspace, "compacted")
    compacted_files = compactor.compact_staging_files(staging_files, uf="MA", output_dir=out_dir)

    assert len(compacted_files) > 0
    total_read_rows = sum(len(pq.ParquetFile(cf).read()) for cf in compacted_files)
    assert total_read_rows == total_expected_rows

    # Valida integridade do schema
    for cf in compacted_files:
        pf = pq.ParquetFile(cf)
        assert pf.schema_arrow.names == ["id", "valor", "nome"]


def test_delta_batch_sink_streaming_commit(temp_workspace):
    """Garante que DeltaBatchSink comita batches via RecordBatchReader para o Delta Lake com schema SIH válido."""
    stg_root = os.path.join(temp_workspace, "staging")
    delta_dir = os.path.join(temp_workspace, "delta_bronze")

    stg = ParquetStagingWriter(subsystem="sih", year=2026, month=5, uf="MA", staging_root=stg_root)
    os.makedirs(stg.staging_dir, exist_ok=True)

    staged_files = []
    for i in range(3):
        f = os.path.join(stg.staging_dir, f"part-{i:06d}.parquet")
        tbl = pa.table({
            "N_AIH": [f"123456{i}1", f"123456{i}2"],
            "VAL_TOT": [100.5 * (i + 1), 200.0 * (i + 1)],
            "CNES": ["2001", "2002"],
            "DT_INTER": ["20260501", "20260502"],
            "DT_SAIDA": ["20260505", "20260506"],
        })
        pq.write_table(tbl, f)
        staged_files.append(f)

    stg.staged_files = staged_files
    stg.total_rows_staged = 6

    sink = DeltaBatchSink(subsystem="sih", year=2026, month=5, uf="MA")
    sink.target_delta_table = delta_dir

    commit_res = sink.commit_staging_to_bronze(stg, execution_id="test-exec-streaming", source_files=staged_files)

    assert commit_res["status"] == "committed"
    assert commit_res["total_rows"] == 6

    dt = DeltaTable(delta_dir)
    arrow_table = dt.to_pyarrow_table()
    assert len(arrow_table) == 6
    assert "fonte" in arrow_table.column_names
    assert "id_execucao" in arrow_table.column_names
    assert "ano" in arrow_table.column_names
    assert "mes" in arrow_table.column_names
    assert "uf" in arrow_table.column_names
