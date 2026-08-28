"""
DAG do Airflow: Geracao e Ingestao de Dados Sinteticos no Padrao FHIR R4.
Gera bundles sinteticos, valida recursos, aplica LGPD e persiste no Delta Bronze.
"""
from datetime import datetime, timedelta
import hashlib
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collectors.fhir_collector import FhirSyntheticCollector
from src.validators.fhir_validator import FhirValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("dag_fhir_synthetic")

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'qimed_fhir_synthetic',
    default_args=default_args,
    description='Pipeline de Geracao e Ingestao FHIR R4 Sintetico -> Bronze Lakehouse',
    schedule_interval='@once',
    catchup=False,
    tags=['fhir', 'synthetic', 'bronze']
)


def run_fhir_ingestion(**kwargs):
    """Executa a geracao, validacao, anonimizacao e persistencia de dados FHIR."""
    params = kwargs.get("params", {})
    num_patients = int(params.get("num_patients", 100))

    logger.info(f"Iniciando geracao sintetica FHIR para {num_patients} pacientes.")
    collector = FhirSyntheticCollector()
    
    # 1. Fetch & Parse
    raw_bundles = collector.fetch()
    df = collector.parse(raw_bundles)
    if df.empty:
        logger.warning("Nenhum registro gerado no coletor FHIR.")
        return {"status": "skipped_empty"}

    # 2. Validate
    validator = FhirValidator()
    val_res = validator.validate(df)
    if val_res.valid_df.empty:
        raise ValueError(f"Validacao de recursos FHIR falhou: {val_res.stats}")

    df_valid = val_res.valid_df

    # 3. LGPD Gate & Anonymization
    detector = PIIDetector()
    pii_fields = detector.detect_pii_fields(source_type="fhir_synthetic", data=df_valid)
    anonymizer = Anonymizer()
    df_anon, _ = anonymizer.anonymize(df_valid, pii_fields=pii_fields)

    # 4. Write Delta Bronze
    writer = BronzeWriter()
    now = datetime.utcnow()
    writer.write(
        df_anon,
        metadata={
            "source_type": "fhir_synthetic",
            "subsystem": "fhir",
            "year": now.year,
            "month": now.month,
            "uf": "BR",
            "source_file": "synthetic_bundle",
        }
    )

    # 5. Catalog Registration
    schema_fingerprint = hashlib.md5("".join(sorted(df_anon.columns)).encode()).hexdigest()
    partition_path = f"fhir/synthetic/year={now.year}/month={now.month:02d}/uf=BR"
    catalog = DatasetCatalog()
    catalog.register_dataset(
        source_type="fhir_synthetic",
        partition_path=partition_path,
        row_count=len(df_anon),
        schema_fingerprint=schema_fingerprint,
        pii_anonymized=True,
        extra_metadata={"num_patients": num_patients, "stats": val_res.stats}
    )

    logger.info(f"Ingestao FHIR concluida com sucesso: {len(df_anon)} recursos gravados.")
    return {"status": "success", "rows_written": len(df_anon)}


t_ingest_fhir = PythonOperator(
    task_id='ingest_fhir_to_bronze',
    python_callable=run_fhir_ingestion,
    dag=dag
)
