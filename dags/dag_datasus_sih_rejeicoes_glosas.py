"""
DAG do Apache Airflow para Ingestão Granular de Glosas e Rejeições do SIH (SIH-RJ e SIH-ER).
Coleta AIHs recusadas e relatórios de críticas de faturamento do DATASUS, persistindo em fct_glosas_hospitalares.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collectors.datasus_collector import DatasusCollector
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.processing.duckdb_engine import DuckDBEngine
from src.processing.transformations import CanonicalTransformations
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("dag_datasus_sih_rejeicoes_glosas")

default_args = {
    "owner": "qimed",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "qimed_datasus_sih_rejeicoes_glosas",
    default_args=default_args,
    description="Ingestão granular de AIHs rejeitadas (SIH-RJ) e críticas (SIH-ER) do DATASUS",
    schedule_interval="@monthly",
    catchup=False,
    tags=["datasus", "sih", "glosas", "rejeicoes", "bronze", "silver"]
)


def run_sih_rj_er_ingestion(**kwargs):
    """Executa download de RJ e ER, anonimização LGPD e persistência na Bronze Delta."""
    params = kwargs.get("params", {})
    uf = params.get("uf", "CE")
    year = int(params.get("year", 2026))
    month = int(params.get("month", 5))

    logger.info(f"Iniciando coleta SIH-RJ e SIH-ER para UF={uf} ({year}/{month:02d})...")
    
    # 1. Coleta SIH-RJ (Rejeitadas)
    col_rj = DatasusCollector(subsystem="SIH-RJ", uf=uf, year=year, month=month)
    raw_rj = col_rj.fetch()
    df_rj = col_rj.parse(raw_rj)
    
    # 2. Coleta SIH-ER (Críticas)
    col_er = DatasusCollector(subsystem="SIH-ER", uf=uf, year=year, month=month)
    raw_er = col_er.fetch()
    df_er = col_er.parse(raw_er)

    # 3. LGPD & Bronze
    detector = PIIDetector()
    anonymizer = Anonymizer()
    writer = BronzeWriter()

    if not df_rj.empty:
        pii_rj = detector.detect_pii_fields("datasus_sih", df_rj)
        df_rj_anon, _ = anonymizer.anonymize(df_rj, pii_fields=pii_rj)
        writer.write(df_rj_anon, metadata={"source_type": "datasus_sih_rj", "subsystem": "sih_rj", "year": year, "month": month, "uf": uf})

    if not df_er.empty:
        pii_er = detector.detect_pii_fields("datasus_sih", df_er)
        df_er_anon, _ = anonymizer.anonymize(df_er, pii_fields=pii_er)
        writer.write(df_er_anon, metadata={"source_type": "datasus_sih_er", "subsystem": "sih_er", "year": year, "month": month, "uf": uf})

    logger.info(f"Bronze SIH-RJ ({len(df_rj)} linhas) e SIH-ER ({len(df_er)} linhas) gravados com sucesso.")
    return {"rj_rows": len(df_rj), "er_rows": len(df_er)}


def run_silver_glosas_transformation(**kwargs):
    """Cruza RJ e ER no DuckDB e materializa a tabela fct_glosas_hospitalares na Silver."""
    logger.info("Materializando fct_glosas_hospitalares na Camada Silver...")
    engine = DuckDBEngine()
    try:
        transforms = CanonicalTransformations(duck_engine=engine)
        transforms.transformar_glosas_hospitalares_para_silver(execution_id="exec_dag_glosas")
    finally:
        engine.close()


def run_catalog_glosas(**kwargs):
    """Registra a linhagem e metadados no catálogo central."""
    catalog = DatasetCatalog()
    catalog.register_dataset(
        name="fct_glosas_hospitalares",
        layer="silver",
        format="delta",
        location="lakehouse/silver/fct_glosas_hospitalares",
        schema={"id_glosa_hospitalar": "string", "numero_aih": "string", "codigo_motivo_glosa": "string", "valor_glosado_brl": "double"},
        description="Fatos de glosas hospitalares e críticas de rejeição do SIH-RJ/ER",
        tags=["sih", "glosas", "rejeicoes", "auditoria"]
    )
    logger.info("Dataset fct_glosas_hospitalares catalogado com sucesso.")


t_ingest = PythonOperator(
    task_id="ingest_sih_rejeicoes_bronze",
    python_callable=run_sih_rj_er_ingestion,
    dag=dag
)

t_transform = PythonOperator(
    task_id="transform_fct_glosas_silver",
    python_callable=run_silver_glosas_transformation,
    dag=dag
)

t_catalog = PythonOperator(
    task_id="catalog_glosas_dataset",
    python_callable=run_catalog_glosas,
    dag=dag
)

t_ingest >> t_transform >> t_catalog
