import os
import pytest
from unittest.mock import patch, MagicMock
import duckdb
import pyarrow as pa

import src.utils.config_loader as config_loader
from src.utils.s3_storage import (
    get_storage_backend,
    is_s3_backend,
    get_s3_bucket_name,
    get_s3_endpoint_url,
    get_s3_storage_options,
    resolve_lakehouse_path,
    configure_duckdb_s3,
    lakehouse_path_exists,
    s3_path_exists,
)
from src.ingestion.batch_sink import DeltaBatchSink
from src.processing.transformations import CanonicalTransformations


@pytest.fixture(autouse=True)
def reset_config_cache():
    config_loader._CONFIG_CACHE = None
    yield
    config_loader._CONFIG_CACHE = None


def test_s3_storage_options_and_backend_detection(monkeypatch):
    # 1. Modo S3 Ativo (Padrão)
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_user")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_pass")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET_NAME", "test-lakehouse")

    assert is_s3_backend() is True
    assert get_s3_bucket_name() == "test-lakehouse"
    assert get_s3_endpoint_url() == "http://minio:9000"

    opts = get_s3_storage_options()
    assert opts is not None
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert opts["AWS_ACCESS_KEY_ID"] == "test_user"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "test_pass"
    assert opts["AWS_REGION"] == "us-east-1"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"

    # 2. Modo Local Fallback
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    assert is_s3_backend() is False
    assert get_s3_storage_options() is None


def test_resolve_lakehouse_path(monkeypatch):
    # 1. Resolução para S3
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET_NAME", "qimed-lakehouse")

    path_bronze = resolve_lakehouse_path("lakehouse/bronze/datasus/sih")
    assert path_bronze == "s3://qimed-lakehouse/bronze/datasus/sih"

    path_silver = resolve_lakehouse_path("silver/fct_internacao")
    assert path_silver == "s3://qimed-lakehouse/silver/fct_internacao"

    # Se já vier como s3://, preserva
    explicit_s3 = "s3://custom-bucket/raw/data"
    assert resolve_lakehouse_path(explicit_s3) == explicit_s3

    # 2. Resolução para Local
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    path_local = resolve_lakehouse_path("bronze/datasus/sih")
    assert path_local == "lakehouse/bronze/datasus/sih"


def test_configure_duckdb_s3(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minio_admin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minio_secret_password")

    conn = duckdb.connect(":memory:")
    configure_duckdb_s3(conn)

    # Verifica se os parâmetros S3 foram definidos no DuckDB
    settings = {row[0]: row[1] for row in conn.execute("SELECT name, value FROM duckdb_settings() WHERE name LIKE 's3_%'").fetchall()}
    assert settings.get("s3_endpoint") == "localhost:9000"
    assert settings.get("s3_access_key_id") == "minio_admin"
    assert settings.get("s3_secret_access_key") == "minio_secret_password"
    assert settings.get("s3_use_ssl") == "false"
    assert settings.get("s3_url_style") == "path"


def test_lakehouse_path_exists_local_and_s3(tmp_path, monkeypatch):
    # 1. Caminho local existente
    local_file = tmp_path / "test.txt"
    local_file.write_text("hello")
    assert lakehouse_path_exists(str(local_file)) is True
    assert s3_path_exists(str(local_file)) is True

    # 2. Caminho local inexistente
    fake_local = tmp_path / "non_existent.parquet"
    assert lakehouse_path_exists(str(fake_local)) is False

    # 3. Caminho S3 simulado com sucesso (DeltaTable mock)
    with patch("deltalake.DeltaTable") as mock_dt:
        mock_dt.return_value = MagicMock()
        assert lakehouse_path_exists("s3://qimed-lakehouse/bronze/datasus/sih") is True

    # 4. Caminho S3 inexistente
    with patch("deltalake.DeltaTable", side_effect=Exception("Table not found")), \
         patch("duckdb.connect") as mock_conn:
        mock_conn.return_value.execute.side_effect = Exception("Object not found")
        assert lakehouse_path_exists("s3://qimed-lakehouse/bronze/datasus/missing_table") is False


def test_batch_sink_injects_s3_storage_options(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET_NAME", "qimed-lakehouse")

    sink = DeltaBatchSink(subsystem="sih", year=2026, month=5, uf="MG")
    sink.validator.validate_staging_files = MagicMock(return_value={"is_valid": True, "total_rows": 3, "error": None})
    sink.manifest_manager.record_manifest_entry = MagicMock()

    mock_staging = MagicMock()
    mock_staging.get_staged_files.return_value = ["fake_staging.parquet"]

    with patch("src.ingestion.batch_sink.write_deltalake") as mock_write, \
         patch("src.ingestion.batch_sink.pq.ParquetFile") as mock_pq, \
         patch("src.ingestion.batch_sink.DeltaCompactor.compact_staging_files", return_value=["fake_staging.parquet"]):

        mock_table = pa.table({"col1": [1, 2, 3]})
        mock_pq.return_value.read.return_value = mock_table

        sink.commit_staging_to_bronze(mock_staging, execution_id="exec-123")

        mock_write.assert_called_once()
        target_path, table_arg = mock_write.call_args[0]
        kwargs = mock_write.call_args[1]

        assert target_path == "s3://qimed-lakehouse/bronze/datasus/sih"
        assert "storage_options" in kwargs
        assert kwargs["storage_options"]["AWS_ENDPOINT_URL"] == "http://minio:9000"
        assert kwargs["storage_options"]["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"


def test_canonical_transformations_injects_s3_storage_options(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_BUCKET_NAME", "qimed-lakehouse")
    monkeypatch.setenv("QIMED_MPI_SALT", "test_mpi_salt_1234567890abcdef")

    transf = CanonicalTransformations()
    mock_table = pa.table({"id": [1, 2], "valor": [10.0, 20.0]})

    with patch("src.processing.transformations.write_deltalake") as mock_write, \
         patch.object(transf.lineage_tracker, "record_lineage"):

        transf._persist_silver_table(
            table_name="fct_internacao",
            arrow_table=mock_table,
            execution_id="exec-transf-001",
            source_entity="bronze_sih",
            mode="overwrite",
            predicate="ano = '2026'"
        )

        mock_write.assert_called_once()
        target_path, table_arg = mock_write.call_args[0]
        kwargs = mock_write.call_args[1]

        assert target_path == "s3://qimed-lakehouse/silver/fct_internacao"
        assert "storage_options" in kwargs
        assert kwargs["storage_options"]["AWS_ENDPOINT_URL"] == "http://minio:9000"
        assert kwargs["storage_options"]["AWS_ALLOW_HTTP"] == "true"
