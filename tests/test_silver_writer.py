"""
Testes unitários para o SilverWriter e persistência no Delta Lake.
"""
import os
import pytest
import pandas as pd
from deltalake import DeltaTable

from src.silver.mappers.base_mapper import CanonicalDataset
from src.lakehouse.silver_writer import SilverWriter


class TestSilverWriter:
    """Testes para o gerenciamento de tabelas Delta Lake pelo SilverWriter."""

    @pytest.fixture
    def silver_tmp_dir(self, tmp_path):
        s_dir = tmp_path / "lakehouse" / "silver"
        s_dir.mkdir(parents=True)
        return str(s_dir)

    def test_write_canonical_dataset(self, silver_tmp_dir):
        """O SilverWriter deve criar tabelas Delta para todas as entidades não vazias."""
        writer = SilverWriter(silver_base_path=silver_tmp_dir)

        dataset = CanonicalDataset(
            dim_patients=pd.DataFrame([
                {"patient_master_id": "mpi_123", "gender": "female", "state": "CE"}
            ]),
            fct_encounters=pd.DataFrame([
                {"encounter_id": "enc_1", "patient_master_id": "mpi_123", "status": "finished"}
            ]),
            fct_conditions=pd.DataFrame([
                {"condition_id": "cond_1", "patient_master_id": "mpi_123", "code": "I10"}
            ]),
        )

        results = writer.write_canonical_dataset(dataset)

        assert results["dim_patients"]["status"] == "success"
        assert results["fct_encounters"]["status"] == "success"
        assert results["fct_conditions"]["status"] == "success"
        assert results["dim_organizations"]["status"] == "skipped_empty"

        # Verifica tabela Delta em disco
        pat_table_path = os.path.join(silver_tmp_dir, "dim_patients")
        assert os.path.exists(pat_table_path)
        dt = DeltaTable(pat_table_path)
        assert len(dt.to_pandas()) == 1

    def test_append_silver_table(self, silver_tmp_dir):
        """Gravações sucessivas devem anexar novos registros sem corromper o histórico."""
        writer = SilverWriter(silver_base_path=silver_tmp_dir)

        df1 = pd.DataFrame([{"patient_master_id": "mpi_1", "state": "SP"}])
        df2 = pd.DataFrame([{"patient_master_id": "mpi_2", "state": "RJ"}])

        writer._write_table(df1, "dim_patients", mode="append")
        writer._write_table(df2, "dim_patients", mode="append")

        pat_table_path = os.path.join(silver_tmp_dir, "dim_patients")
        dt = DeltaTable(pat_table_path)
        assert len(dt.to_pandas()) == 2

    def test_silver_writer_idempotency_no_duplicates(self, silver_tmp_dir):
        """Reexecuções com os mesmos dados não devem duplicar registros na Silver."""
        writer = SilverWriter(silver_base_path=silver_tmp_dir)

        df = pd.DataFrame([
            {"patient_master_id": "mpi_100", "state": "CE", "gender": "female"},
            {"patient_master_id": "mpi_200", "state": "SP", "gender": "male"}
        ])

        # Executa duas vezes
        writer._write_table(df, "dim_patients")
        writer._write_table(df, "dim_patients")

        pat_table_path = os.path.join(silver_tmp_dir, "dim_patients")
        dt = DeltaTable(pat_table_path)
        # Deve ter exatamente 2 registros (0 duplicatas)
        assert len(dt.to_pandas()) == 2
