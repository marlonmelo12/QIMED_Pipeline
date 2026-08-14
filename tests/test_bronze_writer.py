"""Tests for the BronzeWriter (Delta Lake)."""
import os
import sys
import pytest
import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.lakehouse.bronze_writer import BronzeWriter


@pytest.fixture
def writer(tmp_lakehouse):
    """BronzeWriter pointing to a temp directory."""
    return BronzeWriter(lakehouse_path=tmp_lakehouse)


@pytest.fixture
def sample_df():
    """Simple DataFrame for Bronze writes."""
    return pd.DataFrame({
        "patient_id": ["p1", "p2", "p3"],
        "diagnosis": ["A00", "B15", "C34"],
        "value": [100, 200, 300],
        "_record_hash": ["hash_a", "hash_b", "hash_c"],
    })


@pytest.fixture
def metadata():
    return {
        "source": "datasus",
        "subsystem": "sih",
        "source_type": "datasus_sih",
        "source_file": "RDCE2601.dbc",
    }


class TestBronzeWriter:
    """Tests for BronzeWriter."""

    def test_write_creates_delta_table(self, writer, sample_df, metadata):
        """Writing should create a Delta table at the expected path."""
        stats = writer.write(sample_df, metadata)

        assert stats["rows_written"] == 3
        assert os.path.exists(stats["table_path"])

        # Verify it's a valid Delta table
        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()
        assert len(result) == 3

    def test_write_adds_metadata_columns(self, writer, sample_df, metadata):
        """Written data should have ingestion metadata columns."""
        stats = writer.write(sample_df, metadata)

        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()

        assert "_ingested_at" in result.columns
        assert "_source_type" in result.columns
        assert "_source_file" in result.columns
        assert result["_source_type"].iloc[0] == "datasus_sih"
        assert result["_source_file"].iloc[0] == "RDCE2601.dbc"

    def test_write_partitions_correctly(self, writer, sample_df, metadata):
        """Data should be partitioned by year and month."""
        stats = writer.write(sample_df, metadata)

        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()

        assert "year" in result.columns
        assert "month" in result.columns

    def test_append_mode_adds_rows(self, writer, sample_df, metadata):
        """Writing twice should append, not overwrite."""
        writer.write(sample_df, metadata)
        writer.write(sample_df, metadata)

        table_path = os.path.join(writer.lakehouse_path, "datasus", "sih")
        dt = DeltaTable(table_path)
        result = dt.to_pandas()

        # 3 rows x 2 writes = 6 rows (append mode)
        assert len(result) == 6

    def test_empty_df_skips_write(self, writer, metadata):
        """Empty DataFrame should not create a table."""
        empty_df = pd.DataFrame(columns=["col1", "col2"])
        stats = writer.write(empty_df, metadata)

        assert stats["rows_written"] == 0
