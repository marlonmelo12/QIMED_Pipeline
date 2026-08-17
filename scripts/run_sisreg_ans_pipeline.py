"""
Script de Execucao do Pipeline de Ingestao e Transformacao Silver para SISREG e ANS.
1. Coleta e anonimizacao LGPD (SHA-256)
2. Validacao de esquema
3. Gravacao na Camada Bronze (Delta Lake)
4. Transformacao Semantica Silver (fct_referrals e dim_health_plans)
5. Exportacao para o banco SQLite e CSVs
"""
import os
import sys
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.collectors.sisreg_collector import SisregCollector
from src.collectors.ans_collector import AnsCollector
from src.validators.regulation_and_supplementary_validators import SisregValidator, AnsValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.silver.pipeline import SilverTransformationPipeline
from src.utils.logging_config import get_logger

logger = get_logger("run_sisreg_ans_pipeline")

def run_pipeline():
    detector = PIIDetector()
    anonymizer = Anonymizer()
    bronze_writer = BronzeWriter()
    silver_pipeline = SilverTransformationPipeline()

    print("=" * 80)
    print("  PIPELINE DE INTEGRACAO SISREG/CROSS E ANS/D-TISS (ACRE 2025)")
    print("=" * 80)

    # --- 1. SISREG / CROSS (Regulacao e Filas) ---
    logger.info("=== [1/2] Ingestao e Processamento SISREG ===")
    sisreg_col = SisregCollector(uf="AC", year=2025, month=1)
    raw_sisreg = sisreg_col.fetch()
    df_sisreg = sisreg_col.parse(raw_sisreg)
    
    # LGPD Gate
    pii_sisreg = detector.detect_pii_fields("sisreg_regulation", df_sisreg)
    df_sisreg_anon, audit_sisreg = anonymizer.anonymize(df_sisreg, pii_sisreg)
    val_sisreg = SisregValidator().validate(df_sisreg_anon)
    
    # Bronze Write
    bronze_writer.write(val_sisreg.valid_df, {
        "source": "sisreg",
        "subsystem": "regulation",
        "source_type": "sisreg_regulation",
        "source_file": "SISREG_AC_202501.csv"
    })
    
    # Silver Transformation (fct_referrals)
    silver_sisreg = silver_pipeline.transform_dataframe(val_sisreg.valid_df, source_type="sisreg_regulation")
    print(f"-> SISREG / CROSS processado: {len(silver_sisreg.fct_referrals)} solicitacoes de leitos e consultas em fct_referrals.")

    # --- 2. ANS / D-TISS (Saude Suplementar) ---
    logger.info("=== [2/2] Ingestao e Processamento ANS ===")
    ans_col = AnsCollector(uf="AC", year=2025)
    raw_ans = ans_col.fetch()
    df_ans = ans_col.parse(raw_ans)
    
    # LGPD Gate
    pii_ans = detector.detect_pii_fields("ans_data", df_ans)
    df_ans_anon, audit_ans = anonymizer.anonymize(df_ans, pii_ans)
    val_ans = AnsValidator().validate(df_ans_anon)
    
    # Bronze Write
    bronze_writer.write(val_ans.valid_df, {
        "source": "ans",
        "subsystem": "supplementary_health",
        "source_type": "ans_data",
        "source_file": "ANS_BENEFICIARIOS_AC_2025.csv"
    })
    
    # Silver Transformation (dim_health_plans)
    silver_ans = silver_pipeline.transform_dataframe(val_ans.valid_df, source_type="ans_data")
    print(f"-> ANS / D-TISS processado:    {len(silver_ans.dim_health_plans)} registros de operadoras e cobertura privada em dim_health_plans.")

    print("=" * 80)
    print("Processamento concluido com sucesso nas camadas Bronze e Silver!")
    print("=" * 80)

if __name__ == "__main__":
    run_pipeline()
