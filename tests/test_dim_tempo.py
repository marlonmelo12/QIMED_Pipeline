"""
Testes de Unicidade, Idempot?ncia e Granularidade da Tabela dim_tempo (Task 1).
"""
import os
import duckdb
import pandas as pd
import pytest
from src.processing.transformations import CanonicalTransformations
from src.processing.duckdb_engine import DuckDBEngine


@pytest.fixture
def temp_transformations(tmp_path):
    """Instancia CanonicalTransformations apontando para diret?rio tempor?rio."""
    silver_dir = tmp_path / "lakehouse" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    cfg = {
        "paths": {
            "silver_dir": str(silver_dir),
            "bronze_dir": str(tmp_path / "lakehouse" / "bronze"),
        }
    }
    engine = DuckDBEngine(config=cfg)
    return CanonicalTransformations(duck_engine=engine, config=cfg), str(silver_dir)


def test_dim_tempo_unicidade_e_granularidade(temp_transformations):
    """Valida contagem exata (1095 dias), unicidade de data e integridade dos tipos e nulos."""
    transformer, silver_dir = temp_transformations
    transformer.gerar_dim_tempo(start_year=2025, end_year=2027, execution_id="test_exec_1")

    delta_path = os.path.join(silver_dir, "dim_tempo").replace("\\", "/")
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    df = conn.execute(f"""
        SELECT 
            COUNT(*) AS total_linhas,
            COUNT(DISTINCT data) AS total_datas_unicas,
            COUNT(CASE WHEN data IS NULL THEN 1 END) AS nulos_data,
            COUNT(CASE WHEN ano IS NULL THEN 1 END) AS nulos_ano,
            COUNT(CASE WHEN mes IS NULL THEN 1 END) AS nulos_mes,
            COUNT(CASE WHEN dia IS NULL THEN 1 END) AS nulos_dia,
            COUNT(CASE WHEN trimestre IS NULL THEN 1 END) AS nulos_trimestre,
            COUNT(CASE WHEN indicador_dia_util IS NULL THEN 1 END) AS nulos_dia_util,
            MIN(data) AS min_data,
            MAX(data) AS max_data
        FROM delta_scan('{delta_path}')
    """).df()

    assert df["total_linhas"][0] == 1095, f"Esperado 1095 linhas, obteve {df['total_linhas'][0]}"
    assert df["total_datas_unicas"][0] == 1095, f"Esperado 1095 datas ?nicas, obteve {df['total_datas_unicas'][0]}"
    assert df["nulos_data"][0] == 0, "A coluna data n?o pode conter nulos."
    assert df["nulos_ano"][0] == 0, "A coluna ano n?o pode conter nulos."
    assert df["nulos_mes"][0] == 0, "A coluna mes n?o pode conter nulos."
    assert df["nulos_dia"][0] == 0, "A coluna dia n?o pode conter nulos."
    assert df["nulos_trimestre"][0] == 0, "A coluna trimestre n?o pode conter nulos."
    assert df["nulos_dia_util"][0] == 0, "A coluna indicador_dia_util n?o pode conter nulos."
    assert pd.to_datetime(df["min_data"][0]).strftime('%Y-%m-%d') == "2025-01-01", f"Data m?nima esperada 2025-01-01, obteve {df['min_data'][0]}"
    assert pd.to_datetime(df["max_data"][0]).strftime('%Y-%m-%d') == "2027-12-31", f"Data m?xima esperada 2027-12-31, obteve {df['max_data'][0]}"
    conn.close()


def test_dim_tempo_idempotencia(temp_transformations):
    """Valida que chamadas consecutivas de gerar_dim_tempo mant?m exatamente 1095 linhas sem duplicar."""
    transformer, silver_dir = temp_transformations
    # Primeira execu??o
    transformer.gerar_dim_tempo(start_year=2025, end_year=2027, execution_id="test_exec_1")
    # Segunda execu??o consecutiva (idempot?ncia com overwrite)
    transformer.gerar_dim_tempo(start_year=2025, end_year=2027, execution_id="test_exec_2")
    # Terceira execu??o consecutiva
    transformer.gerar_dim_tempo(start_year=2025, end_year=2027, execution_id="test_exec_3")

    delta_path = os.path.join(silver_dir, "dim_tempo").replace("\\", "/")
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    df = conn.execute(f"""
        SELECT 
            COUNT(*) AS total_linhas,
            COUNT(DISTINCT data) AS total_datas_unicas
        FROM delta_scan('{delta_path}')
    """).df()

    assert df["total_linhas"][0] == 1095, f"Idempot?ncia falhou: esperado 1095 linhas ap?s reexecu??es, obteve {df['total_linhas'][0]}"
    assert df["total_datas_unicas"][0] == 1095, f"Esperado 1095 datas ?nicas, obteve {df['total_datas_unicas'][0]}"
    conn.close()
