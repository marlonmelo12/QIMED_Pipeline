"""
DAG do Airflow: Ingestao do CNES/DATASUS (Cadastro Nacional de Estabelecimentos de Saude).
Executa download FTP real, descompressao, validacao, anonimizacao LGPD e persistencia Delta Bronze.
"""
from datetime import datetime, timedelta
import hashlib
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collectors.datasus_collector import DatasusCollector
from src.validators.datasus_validator import DatasusValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("dag_datasus_cnes")

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'qimed_datasus_cnes',
    default_args=default_args,
    description='Pipeline de Ingestao CNES DATASUS -> Bronze Lakehouse',
    schedule_interval='@monthly',
    catchup=False,
    tags=['datasus', 'cnes', 'bronze']
)


def run_cnes_ingestion(**kwargs):
    """Executa o ciclo completo de ingestao e persistencia do CNES para Bronze."""
    params = kwargs.get("params", {})
    uf = params.get("uf", "CE")
    year = int(params.get("year", 2026))
    month = int(params.get("month", 5))

    logger.info(f"Iniciando ingestao CNES para UF={uf} ({year}/{month:02d})")
    collector = DatasusCollector(subsystem="CNES", uf=uf, year=year, month=month)
    
    # 1. Fetch & Parse
    raw_path = collector.fetch()
    df = collector.parse(raw_path)
    if df.empty:
        logger.warning(f"Nenhum registro encontrado para CNES UF={uf}")
        return {"status": "skipped_empty", "uf": uf}

    # 2. Validate (ValidationResult: valid_df, rejected_df, stats)
    validator = DatasusValidator(subsystem="CNES")
    val_res = validator.validate(df)
    if val_res.valid_df.empty:
        raise ValueError(f"Validacao de schema do CNES falhou: {val_res.stats}")

    df_valid = val_res.valid_df

    # 3. LGPD Gate & Anonymization
    detector = PIIDetector()
    pii_fields = detector.detect_pii_fields(source_type="datasus_cnes", data=df_valid)
    anonymizer = Anonymizer()
    df_anon, _ = anonymizer.anonymize(df_valid, pii_fields=pii_fields)

    # 4. Write Delta Bronze
    writer = BronzeWriter()
    res = writer.write(
        df_anon,
        metadata={
            "source_type": "datasus_cnes",
            "subsystem": "cnes",
            "year": year,
            "month": month,
            "uf": uf,
            "source_file": raw_path if isinstance(raw_path, str) else "cnes_ftp",
        }
    )

    # 5. Catalog Registration
    schema_fingerprint = hashlib.md5("".join(sorted(df_anon.columns)).encode()).hexdigest()
    partition_path = f"datasus/cnes/year={year}/month={month:02d}/uf={uf}"
    catalog = DatasetCatalog()
    catalog.register_dataset(
        source_type="datasus_cnes",
        partition_path=partition_path,
        row_count=len(df_anon),
        schema_fingerprint=schema_fingerprint,
        pii_anonymized=True,
        extra_metadata={"uf": uf, "year": year, "month": month, "stats": val_res.stats}
    )

    logger.info(f"Ingestao CNES finalizada com sucesso para {uf}: {len(df_anon)} registros.")
    return {"status": "success", "rows_written": len(df_anon), "uf": uf}


t_ingest_cnes = PythonOperator(
    task_id='ingest_cnes_to_bronze',
    python_callable=run_cnes_ingestion,
    dag=dag
)
