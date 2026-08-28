"""
Teste de Parquet Staging e PreCommitValidator - QIMED Lakehouse V3.
"""
import os
import shutil
import pytest
import pyarrow as pa
from src.ingestion.staging_writer import ParquetStagingWriter
from src.ingestion.pre_commit_validator import PreCommitValidator


def test_staging_writer_and_validator(tmp_path):
    stg_dir = str(tmp_path / "staging")
    writer = ParquetStagingWriter(subsystem="sih", year=2026, month=5, uf="RO", staging_root=stg_dir)

    # Cria lote de teste Arrow
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["12345", "67890"]),
            pa.array(["0001", "0002"]),
            pa.array(["2026-05-01", "2026-05-02"]),
            pa.array(["2026-05-05", "2026-05-06"]),
            pa.array([1500.50, 2300.00]),
        ],
        names=["N_AIH", "CNES", "DT_INTER", "DT_SAIDA", "VAL_TOT"],
    )

    fpath = writer.write_batch(batch)
    assert os.path.exists(fpath)
    assert writer.total_rows_staged == 2

    # Valida??o Pr?-Commit
    validator = PreCommitValidator(subsystem="SIH")
    val_res = validator.validate_staging_files(writer.get_staged_files(), expected_min_rows=1)
    assert val_res["is_valid"] is True
    assert val_res["total_rows"] == 2

    # Limpeza
    writer.cleanup_staging()
    assert len(writer.get_staged_files()) == 0


def test_delta_compactor_streaming(tmp_path):
    from src.lakehouse.compaction import DeltaCompactor
    import pyarrow.parquet as pq

    stg_dir = str(tmp_path / "staging_compaction")
    os.makedirs(stg_dir, exist_ok=True)

    # Cria 3 arquivos parquet de staging
    files = []
    for i in range(3):
        fpath = os.path.join(stg_dir, f"part_{i}.parquet")
        tbl = pa.Table.from_pydict({"id": [f"id_{i}_{j}" for j in range(10)], "val": [j * 1.5 for j in range(10)]})
        pq.write_table(tbl, fpath)
        files.append(fpath)

    compactor = DeltaCompactor()
    out_dir = str(tmp_path / "compacted")
    compacted = compactor.compact_staging_files(files, uf="RO", output_dir=out_dir)

    # RO é small_uf -> deve compactar em 1 arquivo
    assert len(compacted) == 1
    assert os.path.exists(compacted[0])
    res_tbl = pq.read_table(compacted[0])
    assert len(res_tbl) == 30

