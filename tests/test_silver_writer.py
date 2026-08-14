"""
Unit tests for SilverWriter and Delta Lake persistence.
"""
import os
import pytest
import pandas as pd
from deltalake import DeltaTable

from src.silver.mappers.base_mapper import CanonicalDataset
from src.lakehouse.silver_writer import SilverWriter


class TestSilverWriter:
    """Tests for SilverWriter Delta Lake tables."""

    @pytest.fixture
    def silver_tmp_dir(self, tmp_path):
        s_dir = tmp_path / "lakehouse" / "silver"
        s_dir.mkdir(parents=True)
        return str(s_dir)

    def test_write_canonical_dataset(self, silver_tmp_dir):
        """SilverWriter should create Delta tables for all non-empty canonical entities."""
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

        # Verify Delta table on disk
        pat_table_path = os.path.join(silver_tmp_dir, "dim_patients")
        assert os.path.exists(pat_table_path)
        dt = DeltaTable(pat_table_path)
        assert len(dt.to_pandas()) == 1

    def test_append_silver_table(self, silver_tmp_dir):
        """Writing multiple times should append new records."""
        writer = SilverWriter(silver_base_path=silver_tmp_dir)

        df1 = pd.DataFrame([{"patient_master_id": "mpi_1", "state": "SP"}])
        df2 = pd.DataFrame([{"patient_master_id": "mpi_2", "state": "RJ"}])

        writer._write_table(df1, "dim_patients")
        writer._write_table(df2, "dim_patients")

        pat_table_path = os.path.join(silver_tmp_dir, "dim_patients")
        dt = DeltaTable(pat_table_path)
        assert len(dt.to_pandas()) == 2
