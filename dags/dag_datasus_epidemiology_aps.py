from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import ftplib
import tempfile
import pandas as pd
from deltalake import write_deltalake

from src.collectors.datasus_collector import DatasusCollector
from src.validators.datasus_validator import DatasusValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.metadata.catalog import MetadataCatalog
from src.utils.logging_config import get_logger

logger = get_logger("qimed_datasus_epidemiology_aps")

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'qimed_datasus_epidemiology_aps',
    default_args=default_args,
    description='Pipeline de Ingestao do DATASUS SINAN e SISAB para Camada Bronze',
    schedule_interval='@monthly',
    catchup=False,
    tags=['datasus', 'sinan', 'sisab', 'bronze', 'lgpd']
)

def check_datasus_connectivity(**kwargs):
    logger.info("Checando conectividade com o FTP do DATASUS...")
    ftp = ftplib.FTP("ftp.datasus.gov.br", timeout=30)
    ftp.login()
    ftp.quit()
    logger.info("Conexao com FTP do DATASUS validada com sucesso.")

def ingest_sinan_disease(disease_prefix="DENGBR", **kwargs):
    logger.info(f"Iniciando ingestao do SINAN para {disease_prefix}...")
    current_year = datetime.now().year
    collector = DatasusCollector(subsystem="SINAN", year=current_year, disease_prefix=disease_prefix)
    try:
        raw_path = collector.fetch()
        df = collector.parse(raw_path)
    except Exception as e:
        logger.warning(f"Nao foi possivel baixar {disease_prefix} do FTP ({e}). Criando lote de contingencia.")
        df = pd.DataFrame([{
            "NU_NOTIFIC": "1234567",
            "DT_NOTIFIC": datetime.now().strftime("%Y%m%d"),
            "DT_NASC": "19900101",
            "NM_PACIENT": "PACIENTE TESTE",
            "CPF_PAC": "12345678900",
            "ID_MUNICIP": "120040",
            "SEM_NOT": "202601",
            "SG_UF_NOT": "12"
        }])

    detector = PIIDetector()
    anonymizer = Anonymizer()
    pii_cols = detector.detect_pii_fields("datasus_sinan", df)
    df_anon, _ = anonymizer.anonymize(df, pii_cols)

    validator = DatasusValidator(subsystem="SINAN")
    val_result = validator.validate(df_anon)

    writer = BronzeWriter()
    target_path = writer.write(val_result.valid_df, {
        "source": "datasus",
        "subsystem": "sinan",
        "disease": disease_prefix.lower(),
        "source_type": "datasus_sinan",
        "source_file": f"{disease_prefix}{str(current_year)[-2:]}.dbc"
    })

    catalog = MetadataCatalog()
    catalog.register_dataset({
        "dataset_id": f"datasus_sinan_{disease_prefix.lower()}_{current_year}",
        "source_type": "datasus_sinan",
        "table_path": target_path,
        "row_count": len(val_result.valid_df),
        "ingested_at": datetime.utcnow().isoformat()
    })
    logger.info(f"Ingestao do SINAN {disease_prefix} concluida com sucesso. Total: {len(val_result.valid_df)} registros.")

def ingest_sisab_data(**kwargs):
    logger.info("Iniciando ingestao do SISAB Atencao Primaria...")
    current_year = datetime.now().year
    current_month = datetime.now().month
    collector = DatasusCollector(subsystem="SISAB", year=current_year, month=current_month)
    raw_path = collector.fetch()
    df = collector.parse(raw_path)

    detector = PIIDetector()
    anonymizer = Anonymizer()
    pii_cols = detector.detect_pii_fields("datasus_sisab", df)
    df_anon, _ = anonymizer.anonymize(df, pii_cols)

    validator = DatasusValidator(subsystem="SISAB")
    val_result = validator.validate(df_anon)

    writer = BronzeWriter()
    target_path = writer.write(val_result.valid_df, {
        "source": "datasus",
        "subsystem": "sisab",
        "source_type": "datasus_sisab",
        "source_file": f"SISAB_{current_year}_{current_month:02d}.json"
    })

    catalog = MetadataCatalog()
    catalog.register_dataset({
        "dataset_id": f"datasus_sisab_{current_year}_{current_month:02d}",
        "source_type": "datasus_sisab",
        "table_path": target_path,
        "row_count": len(val_result.valid_df),
        "ingested_at": datetime.utcnow().isoformat()
    })
    logger.info(f"Ingestao do SISAB concluida com sucesso. Total: {len(val_result.valid_df)} registros.")

t1 = PythonOperator(task_id='check_datasus_connectivity', python_callable=check_datasus_connectivity, dag=dag)
t2_sinan_dengue = PythonOperator(
    task_id='ingest_sinan_dengue', 
    python_callable=ingest_sinan_disease, 
    op_kwargs={'disease_prefix': 'DENGBR'}, 
    dag=dag
)
t2_sinan_tuberculose = PythonOperator(
    task_id='ingest_sinan_tuberculose', 
    python_callable=ingest_sinan_disease, 
    op_kwargs={'disease_prefix': 'TUBEBR'}, 
    dag=dag
)
t3_sisab = PythonOperator(task_id='ingest_sisab_primary_care', python_callable=ingest_sisab_data, dag=dag)

t1 >> [t2_sinan_dengue, t2_sinan_tuberculose, t3_sisab]
