"""
DAG do Apache Airflow para Ingestao Mensal de Dados Abertos da ANS / D-TISS (Saude Suplementar).
Captura beneficiarios, sinistralidade e despesa assistencial privada por municipio.
"""
from datetime import datetime, timedelta
import hashlib
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collectors.ans_collector import AnsCollector
from src.validators.regulation_and_supplementary_validators import AnsValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.silver.pipeline import SilverTransformationPipeline
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("dag_ans_supplementary_health")

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'qimed_ans_supplementary_health',
    default_args=default_args,
    description='Pipeline de ingestao de dados de saude suplementar, beneficiarios e operadoras da ANS',
    schedule_interval='@monthly',
    catchup=False,
    tags=['ans', 'supplementary_health', 'operadoras', 'bronze', 'silver']
)


def run_ans_pipeline(**kwargs):
    """Executa o ciclo completo de ingestao ANS -> Bronze -> Silver."""
    params = kwargs.get("params", {})
    uf = params.get("uf", "CE")
    year = int(params.get("year", 2026))
    month = int(params.get("month", 5))
    modalidade = params.get("modalidade", "operadoras")

    logger.info(f"Iniciando pipeline ANS ({modalidade}) para UF={uf} ({year}/{month:02d})")
    collector = AnsCollector(modalidade=modalidade, uf=uf, year=year, month=month)
    
    # 1. Fetch & Parse
    raw_data = collector.fetch()
    df = collector.parse(raw_data)
    if df.empty:
        logger.warning(f"Nenhum registro encontrado na ANS para modalidade={modalidade}, UF={uf}")
        return {"status": "skipped_empty", "modalidade": modalidade}

    # 2. LGPD Gate & Anonymization
    detector = PIIDetector()
    pii_fields = detector.detect_pii_fields(source_type=f"ans_{modalidade}", data=df)
    anonymizer = Anonymizer()
    df_anon, _ = anonymizer.anonymize(df, pii_fields=pii_fields)

    # 3. Write Delta Bronze
    writer = BronzeWriter()
    res_bronze = writer.write(
        df_anon,
        metadata={
            "source_type": f"ans_{modalidade}",
            "subsystem": "ans",
            "year": year,
            "month": month,
            "uf": uf,
            "source_file": f"ans_{modalidade}_{uf}_{year}_{month:02d}",
        }
    )

    # 4. Transform to Silver
    silver_pipe = SilverTransformationPipeline()
    canonical = silver_pipe.transform_dataframe(df_anon, source_type="ans_data", source_file=f"ans_{modalidade}")

    # 5. Catalog Registration
    schema_fingerprint = hashlib.md5("".join(sorted(df_anon.columns)).encode()).hexdigest()
    partition_path = f"ans/{modalidade}/year={year}/month={month:02d}/uf={uf}"
    catalog = DatasetCatalog()
    catalog.register_dataset(
        source_type=f"ans_{modalidade}",
        partition_path=partition_path,
        row_count=len(df_anon),
        schema_fingerprint=schema_fingerprint,
        pii_anonymized=True,
        extra_metadata={"uf": uf, "year": year, "month": month, "modalidade": modalidade}
    )

    logger.info(f"Pipeline ANS concluido com sucesso: {len(df_anon)} registros processados.")
    return {"status": "success", "rows_written": len(df_anon), "modalidade": modalidade}


t_ans_pipeline = PythonOperator(
    task_id='run_ans_pipeline',
    python_callable=run_ans_pipeline,
    dag=dag
)
