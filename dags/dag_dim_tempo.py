"""
DAG do Apache Airflow para Geração e Atualização da Dimensão de Tempo (dim_tempo).
Constrói o calendário de datas, dias úteis, meses, trimestres e semestres no formato Delta Lake na Camada Silver.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.processing.duckdb_engine import DuckDBEngine
from src.processing.transformations import CanonicalTransformations
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("dag_dim_tempo")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "qimed_dim_tempo_generator",
    default_args=default_args,
    description="Geração e atualização do calendário canônico dim_tempo na Camada Silver",
    schedule_interval="@monthly",
    catchup=False,
    tags=["silver", "dimensao", "tempo", "calendario"]
)


def run_generate_dim_tempo(**kwargs):
    """Gera a dimensão de tempo para uma janela de anos configurável."""
    params = kwargs.get("params", {})
    start_year = int(params.get("start_year", 2024))
    end_year = int(params.get("end_year", 2028))

    logger.info(f"Gerando dim_tempo para o intervalo {start_year} a {end_year}...")
    engine = DuckDBEngine()
    try:
        transforms = CanonicalTransformations(duck_engine=engine)
        res = transforms.gerar_dim_tempo(start_year=start_year, end_year=end_year, execution_id="exec_dim_tempo")
        logger.info(f"dim_tempo gerada com sucesso: {res}")
        return res
    finally:
        engine.close()


def run_catalog_dim_tempo(**kwargs):
    """Registra a dim_tempo no catálogo central de datasets."""
    catalog = DatasetCatalog()
    catalog.register_dataset(
        name="dim_tempo",
        layer="silver",
        format="delta",
        location="lakehouse/silver/dim_tempo",
        schema={"data": "date", "ano": "integer", "mes": "integer", "trimestre": "integer", "fl_dia_util": "boolean"},
        description="Dimensão canônica de datas, trimestres, semestres e dias úteis",
        tags=["silver", "dimensao", "tempo"]
    )
    logger.info("Dataset dim_tempo catalogado com sucesso.")


t_generate = PythonOperator(
    task_id="generate_dim_tempo_delta",
    python_callable=run_generate_dim_tempo,
    dag=dag
)

t_catalog = PythonOperator(
    task_id="catalog_dim_tempo",
    python_callable=run_catalog_dim_tempo,
    dag=dag
)

t_generate >> t_catalog
