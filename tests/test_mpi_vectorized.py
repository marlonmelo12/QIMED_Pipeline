"""
Teste do MPI em 4 N?veis Vetorizado - QIMED Lakehouse V3.
"""
import pytest
from src.mpi.patient_identity import PatientIdentityResolver
from src.processing.duckdb_engine import DuckDBEngine


def test_mpi_4_levels_resolution():
    engine = DuckDBEngine()
    engine.execute_sql("""
        CREATE TABLE test_atendimentos AS
        SELECT
            '123456789' AS numero_aih,
            NULL AS numero_documento_autorizacao,
            'CNES_01' AS codigo_estabelecimento_cnes,
            '2026-05-01' AS data_internacao,
            '1985-04-12' AS data_nascimento_paciente,
            'M' AS sexo_biologico,
            '355030' AS codigo_municipio_residencia_paciente;
    """)

    resolver = PatientIdentityResolver(duck_engine=engine)
    arrow_res = resolver.apply_vectorized_resolution("test_atendimentos")
    df = arrow_res.to_pandas()

    assert "identificador_atendimento" in df.columns
    assert "identificador_registro" in df.columns
    assert "pseudonimo_paciente" in df.columns
    assert "identificador_paciente_candidato" in df.columns

    assert df["identificador_atendimento"].iloc[0] == "123456789"
    assert len(df["pseudonimo_paciente"].iloc[0]) in (32, 64)
    assert df["identificador_paciente_candidato"].iloc[0].startswith("pac_")
    engine.close()
