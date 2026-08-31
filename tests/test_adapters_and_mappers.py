import os
import shutil
import tempfile
import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion.adapters.file_upload_adapter import FileUploadAdapter
from src.ingestion.adapters.postgres_client_adapter import PostgresClientAdapter
from src.processing.mappers.schema_registry import SchemaRegistry
from src.processing.mappers.canonical_transformer import CanonicalTransformer


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="qimed_test_adapters_")
    yield temp_path
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path, ignore_errors=True)


def test_file_upload_adapter_csv(temp_dir):
    csv_file = os.path.join(temp_dir, "raw_sample.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("CD_ATENDIMENTO,CD_CNES,DT_ENTRADA,VL_CONTA\n")
        f.write("1001,7042671,2026-05-01,1500.50\n")
        f.write("1002,7042671,2026-05-02,2300.00\n")

    adapter = FileUploadAdapter(file_path=csv_file)
    
    # Test connection
    assert asyncio.run(adapter.test_connection()) is True

    # Test discover schema
    schema = asyncio.run(adapter.discover_schema())
    assert "CD_ATENDIMENTO" in schema
    assert "VL_CONTA" in schema

    # Test raw extraction (As-Is)
    arrow_table = asyncio.run(adapter.extract())
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 2
    assert "CD_ATENDIMENTO" in arrow_table.column_names
    assert "VL_CONTA" in arrow_table.column_names

    # Test write bronze
    bronze_dir = os.path.join(temp_dir, "bronze", "tasy", "raw_sample")
    dest = asyncio.run(adapter.write_bronze(arrow_table, bronze_dir))
    assert os.path.exists(dest)


def test_file_upload_adapter_parquet(temp_dir):
    pq_file = os.path.join(temp_dir, "raw_sample.parquet")
    raw_data = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array(["A", "B"])],
        names=["ORIG_COL1", "ORIG_COL2"]
    )
    pq.write_table(raw_data, pq_file)

    adapter = FileUploadAdapter(file_path=pq_file)
    assert asyncio.run(adapter.test_connection()) is True
    
    extracted = asyncio.run(adapter.extract())
    assert extracted.num_rows == 2
    assert extracted.column_names == ["ORIG_COL1", "ORIG_COL2"]


def test_postgres_client_adapter():
    adapter = PostgresClientAdapter(connection_url="postgresql://user:pass@localhost:5432/db")
    assert asyncio.run(adapter.test_connection()) is True
    schema = asyncio.run(adapter.discover_schema("internacoes"))
    assert schema["tabela"] == "internacoes"


def test_schema_registry():
    registry = SchemaRegistry()
    
    tasy_map = registry.get_mapping("tasy")
    assert tasy_map["cd_atendimento"] == "numero_aih"
    assert tasy_map["vl_total_conta"] == "valor_total_brl"

    mv_map = registry.get_mapping("mv_soul")
    assert mv_map["nr_atendimento"] == "numero_aih"
    assert mv_map["sn_obito"] == "indicador_obito"

    csv_map = registry.get_mapping("generic_csv")
    assert csv_map["N_AIH"] == "numero_aih"
    assert csv_map["MORTE"] == "indicador_obito"

    # Custom registration
    registry.register_mapping("custom_erp", {"cod_intern": "numero_aih"})
    assert registry.get_mapping("custom_erp")["cod_intern"] == "numero_aih"


def test_canonical_transformer_tasy():
    transformer = CanonicalTransformer(mpi_salt="test_salt_123")

    # Tasy Raw Table com sentinelas
    raw_tasy = pa.Table.from_arrays(
        [
            pa.array(["ATEND_001", "ATEND_002", "ATEND_003"]),
            pa.array(["7042671", "7042671", "7042671"]),
            pa.array(["2026-05-01", "2026-05-02", "2026-05-03"]),
            pa.array(["2026-05-05", "2026-05-06", "2026-05-07"]),
            pa.array(["J18.0", "0000", "NULL"]),           # CID com sentinelas
            pa.array([45, 999, 150]),                      # Idade com sentinelas (999, 150 -> NULL)
            pa.array(["M", "FEMININO", "UNKNOWN"]),        # Sexo
            pa.array(["S", "0", "N"]),                     # Óbito
            pa.array([1250.50, 3400.00, 500.00]),          # Valor Total
            pa.array(["12345678900", "98765432100", ""]),  # CPF
        ],
        names=[
            "CD_ATENDIMENTO",
            "CD_CNES",
            "DT_ENTRADA",
            "DT_ALTA",
            "CD_CID_PRINCIPAL",
            "NR_IDADE",
            "IE_SEXO",
            "IE_OBITO",
            "VL_TOTAL_CONTA",
            "CD_PESSOA_FISICA"
        ]
    )

    canonical = transformer.transform_to_fct_internacao(raw_tasy, source_format="tasy")

    assert canonical.num_rows == 3
    
    # Valida presença das colunas canônicas
    cols = canonical.column_names
    assert "id_atendimento" in cols
    assert "id_paciente" in cols
    assert "id_registro" in cols
    assert "numero_aih" in cols
    assert "codigo_cid10_principal" in cols
    assert "idade_anos" in cols
    assert "indicador_obito" in cols

    # Valida mapeamento de De-Para
    assert canonical.column("numero_aih").to_pylist() == ["ATEND_001", "ATEND_002", "ATEND_003"]
    assert canonical.column("codigo_estabelecimento_cnes").to_pylist() == ["7042671", "7042671", "7042671"]

    # Valida sanitização de CID (0000 e NULL viram None)
    cids = canonical.column("codigo_cid10_principal").to_pylist()
    assert cids[0] == "J18.0"
    assert cids[1] is None
    assert cids[2] is None

    # Valida sanitização de Idades (999 e 150 viram None)
    idades = canonical.column("idade_anos").to_pylist()
    assert idades[0] == 45
    assert idades[1] is None
    assert idades[2] is None

    # Valida sanitização de Sexo ('M', 'F', 'I')
    sexos = canonical.column("sexo_biologico").to_pylist()
    assert sexos == ["M", "F", "I"]

    # Valida sanitização de Óbito (S -> 1, 0 -> 0, N -> 0)
    obitos = canonical.column("indicador_obito").to_pylist()
    assert obitos == [1, 0, 0]

    # Valida Chaves SHA-256 (64 hex characters)
    for key_col in ["id_atendimento", "id_paciente", "id_registro"]:
        vals = canonical.column(key_col).to_pylist()
        for v in vals:
            assert isinstance(v, str) and len(v) == 64


def test_canonical_transformer_mv_soul():
    transformer = CanonicalTransformer(mpi_salt="mv_salt_456")

    raw_mv = pa.Table.from_arrays(
        [
            pa.array(["MV_9901"]),
            pa.array(["2005432"]),
            pa.array(["2026-05-10"]),
            pa.array(["2026-05-14"]),
            pa.array(["I10"]),
            pa.array([4]),
            pa.array(["1"]),      # Óbito Sim
            pa.array([5500.80]),
            pa.array([1200.00]),
            pa.array(["11122233344"])
        ],
        names=[
            "NR_ATENDIMENTO",
            "CD_HOSPITAL",
            "DT_ATENDIMENTO",
            "DT_ALTA_MEDICA",
            "CD_CID",
            "QT_DIAS",
            "SN_OBITO",
            "VL_TOTAL",
            "VL_UTI",
            "CD_PACIENTE"
        ]
    )

    canonical = transformer.transform_to_fct_internacao(raw_mv, source_format="mv_soul")

    assert canonical.num_rows == 1
    assert canonical.column("numero_aih").to_pylist() == ["MV_9901"]
    assert canonical.column("codigo_estabelecimento_cnes").to_pylist() == ["2005432"]
    assert canonical.column("codigo_cid10_principal").to_pylist() == ["I10"]
    assert canonical.column("indicador_obito").to_pylist() == [1]
    assert canonical.column("dias_permanencia_real").to_pylist() == [4]
    assert canonical.column("valor_total_brl").to_pylist() == [5500.80]
    assert canonical.column("valor_uti_brl").to_pylist() == [1200.00]
