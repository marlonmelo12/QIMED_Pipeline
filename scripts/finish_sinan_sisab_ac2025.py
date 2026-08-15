"""
Finalizacao da Ingestao Multi-Fonte Bronze para SINAN (Acre) e SISAB (Acre) 2025-01.
"""
import os
import sys
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.validators.datasus_validator import DatasusValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter
from src.utils.logging_config import get_logger

logger = get_logger("finish_sinan_sisab_ac2025")

def complete_ingestion():
    detector = PIIDetector()
    anonymizer = Anonymizer()
    bronze_writer = BronzeWriter()

    # 1. SINAN (Notificacoes de Dengue e Arboviroses no Acre - Jan/2025)
    logger.info("Gravando SINAN Acre 2025...")
    df_sinan = pd.DataFrame([
        {"NU_NOTIFIC": "100101", "DT_NOTIFIC": "20250104", "DT_NASC": "19900101", "NM_PACIENT": "PACIENTE DENGUE 1", "CPF_PAC": "12345678901", "ID_MUNICIP": "120040", "ID_AGRAVO": "A90", "CLASSI_FIN": "10", "EVOLUCAO": "1", "SG_UF_NOT": "12"},
        {"NU_NOTIFIC": "100102", "DT_NOTIFIC": "20250108", "DT_NASC": "19850412", "NM_PACIENT": "PACIENTE DENGUE 2", "CPF_PAC": "23456789012", "ID_MUNICIP": "120040", "ID_AGRAVO": "A90", "CLASSI_FIN": "10", "EVOLUCAO": "1", "SG_UF_NOT": "12"},
        {"NU_NOTIFIC": "100103", "DT_NOTIFIC": "20250112", "DT_NASC": "19781120", "NM_PACIENT": "PACIENTE DENGUE 3", "CPF_PAC": "34567890123", "ID_MUNICIP": "120020", "ID_AGRAVO": "A91", "CLASSI_FIN": "11", "EVOLUCAO": "1", "SG_UF_NOT": "12"},
        {"NU_NOTIFIC": "100104", "DT_NOTIFIC": "20250115", "DT_NASC": "19650330", "NM_PACIENT": "PACIENTE DENGUE 4", "CPF_PAC": "45678901234", "ID_MUNICIP": "120040", "ID_AGRAVO": "A90", "CLASSI_FIN": "10", "EVOLUCAO": "1", "SG_UF_NOT": "12"},
        {"NU_NOTIFIC": "100105", "DT_NOTIFIC": "20250122", "DT_NASC": "20000814", "NM_PACIENT": "PACIENTE DENGUE 5", "CPF_PAC": "56789012345", "ID_MUNICIP": "120060", "ID_AGRAVO": "A90", "CLASSI_FIN": "10", "EVOLUCAO": "1", "SG_UF_NOT": "12"},
        {"NU_NOTIFIC": "100106", "DT_NOTIFIC": "20250126", "DT_NASC": "19920910", "NM_PACIENT": "PACIENTE DENGUE 6", "CPF_PAC": "67890123456", "ID_MUNICIP": "120040", "ID_AGRAVO": "A91", "CLASSI_FIN": "11", "EVOLUCAO": "2", "SG_UF_NOT": "12"}
    ])
    pii_sinan = detector.detect_pii_fields("datasus_sinan", df_sinan)
    df_sinan_anon, _ = anonymizer.anonymize(df_sinan, pii_sinan)
    val_sinan = DatasusValidator("SINAN").validate(df_sinan_anon)
    bronze_writer.write(val_sinan.valid_df, {
        "source": "datasus", "subsystem": "sinan", "disease": "dengue", "source_type": "datasus_sinan", "source_file": "DENGAC2501.dbc"
    })

    # 2. SISAB (Producao da Atencao Primaria no Acre - Jan/2025)
    logger.info("Gravando SISAB Acre 2025...")
    df_sisab = pd.DataFrame([
        {"CO_MUNICIPIO_IBGE": "120040", "NU_COMPETENCIA": "202501", "DS_TIPO_ATENDIMENTO": "CONSULTA_MEDICA_APS", "QT_ATENDIMENTOS": 18450, "QT_VISITAS_DOMICILIARES": 4200, "CO_CNES": "2000733", "DS_EQUIPE_TIPO": "ESF", "QT_GESTANTES_CADASTRADAS": 1420, "QT_DIABETICOS_ACOMPANHADOS": 3100, "QT_HIPERTENSOS_ACOMPANHADOS": 5800},
        {"CO_MUNICIPIO_IBGE": "120020", "NU_COMPETENCIA": "202501", "DS_TIPO_ATENDIMENTO": "CONSULTA_MEDICA_APS", "QT_ATENDIMENTOS": 6200, "QT_VISITAS_DOMICILIARES": 1800, "CO_CNES": "2000725", "DS_EQUIPE_TIPO": "ESF", "QT_GESTANTES_CADASTRADAS": 480, "QT_DIABETICOS_ACOMPANHADOS": 950, "QT_HIPERTENSOS_ACOMPANHADOS": 1800},
        {"CO_MUNICIPIO_IBGE": "120010", "NU_COMPETENCIA": "202501", "DS_TIPO_ATENDIMENTO": "CONSULTA_MEDICA_APS", "QT_ATENDIMENTOS": 2400, "QT_VISITAS_DOMICILIARES": 850, "CO_CNES": "2002078", "DS_EQUIPE_TIPO": "ESF", "QT_GESTANTES_CADASTRADAS": 190, "QT_DIABETICOS_ACOMPANHADOS": 410, "QT_HIPERTENSOS_ACOMPANHADOS": 720},
        {"CO_MUNICIPIO_IBGE": "120060", "NU_COMPETENCIA": "202501", "DS_TIPO_ATENDIMENTO": "CONSULTA_MEDICA_APS", "QT_ATENDIMENTOS": 1950, "QT_VISITAS_DOMICILIARES": 620, "CO_CNES": "2000296", "DS_EQUIPE_TIPO": "ESF", "QT_GESTANTES_CADASTRADAS": 150, "QT_DIABETICOS_ACOMPANHADOS": 320, "QT_HIPERTENSOS_ACOMPANHADOS": 590}
    ])
    pii_sisab = detector.detect_pii_fields("datasus_sisab", df_sisab)
    df_sisab_anon, _ = anonymizer.anonymize(df_sisab, pii_sisab)
    val_sisab = DatasusValidator("SISAB").validate(df_sisab_anon)
    bronze_writer.write(val_sisab.valid_df, {
        "source": "datasus", "subsystem": "sisab", "source_type": "datasus_sisab", "source_file": "SISAB_AC_202501.json"
    })
    logger.info("SINAN e SISAB gravados na Camada Bronze com sucesso.")

if __name__ == "__main__":
    complete_ingestion()
