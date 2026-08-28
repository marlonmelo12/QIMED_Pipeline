"""Testes unitários para o BronzeWriter (Delta Lake)."""
import os
import sys
import pytest
import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.lakehouse.bronze_writer import BronzeWriter


@pytest.fixture
def writer(tmp_lakehouse):
    """Instância do BronzeWriter apontando para um diretório temporário."""
    return BronzeWriter(lakehouse_path=tmp_lakehouse)


@pytest.fixture
def sample_df():
    """DataFrame de exemplo para testes de gravação na Bronze."""
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
    """Testes para o BronzeWriter."""

    def test_write_creates_delta_table(self, writer, sample_df, metadata):
        """A gravação deve criar uma tabela Delta no caminho esperado."""
        stats = writer.write(sample_df, metadata)

        assert stats["rows_written"] == 3
        assert os.path.exists(stats["table_path"])

        # Verifica se é uma tabela Delta válida
        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()
        assert len(result) == 3

    def test_write_adds_metadata_columns(self, writer, sample_df, metadata):
        """Os dados gravados devem conter colunas de metadados de ingestão."""
        stats = writer.write(sample_df, metadata)

        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()

        assert "_ingested_at" in result.columns
        assert "_source_type" in result.columns
        assert "_source_file" in result.columns
        assert result["_source_type"].iloc[0] == "datasus_sih"
        assert result["_source_file"].iloc[0] == "RDCE2601.dbc"

    def test_write_partitions_correctly(self, writer, sample_df, metadata):
        """Os dados devem ser particionados por ano e mês."""
        stats = writer.write(sample_df, metadata)

        dt = DeltaTable(stats["table_path"])
        result = dt.to_pandas()

        assert "year" in result.columns
        assert "month" in result.columns

    def test_append_mode_adds_rows(self, writer, sample_df, metadata):
        """Gravar duas vezes deve anexar (append), sem sobrescrever."""
        writer.write(sample_df, metadata)
        writer.write(sample_df, metadata)

        table_path = os.path.join(writer.lakehouse_path, "datasus", "sih")
        dt = DeltaTable(table_path)
        result = dt.to_pandas()

        # 3 linhas x 2 gravações = 6 linhas (modo append)
        assert len(result) == 6

    def test_empty_df_skips_write(self, writer, metadata):
        """DataFrame vazio não deve criar tabela."""
        empty_df = pd.DataFrame(columns=["col1", "col2"])
        stats = writer.write(empty_df, metadata)

        assert stats["rows_written"] == 0
