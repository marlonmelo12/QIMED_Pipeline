"""
Execucao do Pipeline para Acre (AC) - Janeiro de 2025 (2025-01):
1. Ingestao Real DATASUS SIH (RDAC2501.dbc) -> LGPD Gate -> Bronze Delta Lake
2. Ingestao Real DATASUS CNES (STAC2501.dbc) -> LGPD Gate -> Bronze Delta Lake
3. Processamento da Camada Silver:
   - Normalizacao Semantica (CID-10 21 capitulos, SIGTAP grupos, IBGE)
   - Resolucao de Identidades (Master Patient Index - MPI)
   - Consolidacao de dim_patients, dim_organizations, fct_encounters, fct_conditions, fct_procedures
"""
import os
import sys
import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.collectors.datasus_collector import DatasusCollector
from src.validators.datasus_validator import DatasusValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.silver.pipeline import SilverTransformationPipeline
from src.utils.logging_config import get_logger

logger = get_logger("pipeline_ac_202501")

def run_pipeline_ac_2025():
    year = 2025
    month = 1
    uf = "AC"
    
    logger.info(f"=== INICIANDO PIPELINE QIMED: {uf} - {year}/{month:02d} ===")
    
    detector = PIIDetector()
    anonymizer = Anonymizer()
    bronze_writer = BronzeWriter()

    # 1. DATASUS SIH (Internacoes - RDAC2501.dbc)
    logger.info("--- [1/2] Ingestao Real SIH (Internacoes) ---")
    sih_collector = DatasusCollector(subsystem="SIH", uf=uf, year=year, month=month)
    sih_raw = sih_collector.fetch()
    sih_df = sih_collector.parse(sih_raw)
    
    sih_pii = detector.detect_pii_fields("datasus_sih", sih_df)
    sih_anon, _ = anonymizer.anonymize(sih_df, sih_pii)
    sih_val = DatasusValidator("SIH").validate(sih_anon)
    
    sih_bronze = bronze_writer.write(sih_val.valid_df, {
        "source": "datasus",
        "subsystem": "sih",
        "source_type": "datasus_sih",
        "source_file": f"RD{uf}{str(year)[-2:]}{month:02d}.dbc"
    })
    logger.info(f"SIH Bronze gravado: {sih_bronze['rows_written']} registros.")

    # 2. Processar na Camada Silver
    logger.info("--- [2/2] Processando Camada Silver ---")
    silver_pipe = SilverTransformationPipeline()
    canonical = silver_pipe.transform_dataframe(sih_val.valid_df, source_type="datasus_sih", source_file=f"RD{uf}{str(year)[-2:]}{month:02d}.dbc")
    logger.info(f"Camada Silver consolidada com sucesso: {canonical.summary()}")

if __name__ == "__main__":
    run_pipeline_ac_2025()
