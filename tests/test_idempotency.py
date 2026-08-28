"""
Teste de Idempotência - QIMED Lakehouse V3.
Garante que reprocessar a mesma partição (ex: SIH 2026/05 RO) não duplica registros no Delta Lake.
"""
import os
import pytest
import pyarrow as pa
from src.ingestion.staging_writer import ParquetStagingWriter
from src.ingestion.batch_sink import DeltaBatchSink
from deltalake import DeltaTable


def test_idempotency_no_duplicates(tmp_path):
    stg_dir = str(tmp_path / "staging")
    bronze_dir = str(tmp_path / "bronze")

    # 1. Primeira Execução
    writer1 = ParquetStagingWriter(subsystem="sih", year=2026, month=5, uf="RO", staging_root=stg_dir)
    batch1 = pa.RecordBatch.from_arrays(
        [
            pa.array(["11111", "22222"]),
            pa.array(["CNES1", "CNES2"]),
            pa.array(["2026-05-01", "2026-05-02"]),
            pa.array(["2026-05-05", "2026-05-06"]),
            pa.array([100.0, 200.0]),
        ],
        names=["N_AIH", "CNES", "DT_INTER", "DT_SAIDA", "VAL_TOT"],
    )
    writer1.write_batch(batch1)

    sink = DeltaBatchSink(subsystem="sih", year=2026, month=5, uf="RO", bronze_root=bronze_dir)
    sink.commit_staging_to_bronze(writer1, execution_id="exec_001")

    # Verifica total de linhas da primeira execução
    dt = DeltaTable(os.path.join(bronze_dir, "datasus", "sih"))
    assert len(dt.to_pandas()) == 2

    # 2. Segunda Execução com os mesmos dados / partição (deve sobrescrever e manter 2 linhas)
    writer2 = ParquetStagingWriter(subsystem="sih", year=2026, month=5, uf="RO", staging_root=stg_dir)
    writer2.write_batch(batch1)
    sink.commit_staging_to_bronze(writer2, execution_id="exec_002")

    dt2 = DeltaTable(os.path.join(bronze_dir, "datasus", "sih"))
    assert len(dt2.to_pandas()) == 2
