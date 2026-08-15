"""
Testes unitarios e integrados para coletores e validadores do SINAN e SISAB.
"""
import pytest
import pandas as pd
import tempfile
import os

from src.collectors.datasus_collector import DatasusCollector
from src.validators.datasus_validator import DatasusValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.lakehouse.bronze_writer import BronzeWriter


def test_datasus_validator_sinan():
    validator = DatasusValidator(subsystem="SINAN")
    
    # Valid dataframe
    df_valid = pd.DataFrame([{
        "NU_NOTIFIC": "1234567",
        "DT_NOTIFIC": "20260115",
        "ID_MUNICIP": "120040"
    }])
    result = validator.validate(df_valid)
    assert len(result.valid_df) == 1
    assert len(result.rejected_df) == 0

    # Missing required column
    df_invalid = pd.DataFrame([{
        "NU_NOTIFIC": "1234567"
    }])
    result_invalid = validator.validate(df_invalid)
    assert len(result_invalid.valid_df) == 0
    assert len(result_invalid.rejected_df) == 1


def test_datasus_validator_sisab():
    validator = DatasusValidator(subsystem="SISAB")
    
    df_valid = pd.DataFrame([{
        "CO_MUNICIPIO_IBGE": "120040",
        "NU_COMPETENCIA": "202601"
    }])
    result = validator.validate(df_valid)
    assert len(result.valid_df) == 1
    assert len(result.rejected_df) == 0


def test_lgpd_gate_sinan_and_sisab():
    detector = PIIDetector()
    anonymizer = Anonymizer(salt="test_salt_123")

    # SINAN
    df_sinan = pd.DataFrame([{
        "NU_NOTIFIC": "1234567",
        "NM_PACIENT": "PACIENTE TESTE",
        "CPF_PAC": "12345678900",
        "DT_NASC": "19800510",
        "ID_MUNICIP": "120040"
    }])
    pii_cols = detector.detect_pii_fields("datasus_sinan", df_sinan)
    assert "NM_PACIENT" in pii_cols
    assert "CPF_PAC" in pii_cols
    assert "DT_NASC" in pii_cols
    assert "NU_NOTIFIC" in pii_cols

    df_anon, _ = anonymizer.anonymize(df_sinan, pii_cols)
    assert df_anon["NM_PACIENT"].iloc[0] != "PACIENTE TESTE"
    assert len(df_anon["NM_PACIENT"].iloc[0]) == 64
    assert df_anon["ID_MUNICIP"].iloc[0] == "120040"  # Non-PII preserved

    # SISAB
    df_sisab = pd.DataFrame([{
        "CO_MUNICIPIO_IBGE": "120040",
        "NU_COMPETENCIA": "202601",
        "NU_CPF_PROFISSIONAL": "98765432100",
        "CO_CNES": "2000733"
    }])
    pii_sisab = detector.detect_pii_fields("datasus_sisab", df_sisab)
    assert "NU_CPF_PROFISSIONAL" in pii_sisab
    assert "CO_CNES" in pii_sisab

    df_sisab_anon, _ = anonymizer.anonymize(df_sisab, pii_sisab)
    assert len(df_sisab_anon["NU_CPF_PROFISSIONAL"].iloc[0]) == 64


def test_sisab_collector_and_bronze_writer():
    collector = DatasusCollector(subsystem="SISAB", year=2026, month=1)
    raw_path = collector.fetch()
    df = collector.parse(raw_path)

    assert not df.empty
    assert "CO_MUNICIPIO_IBGE" in df.columns

    with tempfile.TemporaryDirectory() as temp_dir:
        writer = BronzeWriter(lakehouse_path=temp_dir)
        write_stats = writer.write(df, {
            "source": "datasus",
            "subsystem": "sisab",
            "source_type": "datasus_sisab",
            "source_file": "test_sisab.json"
        })
        assert write_stats["rows_written"] == 1
        assert os.path.exists(write_stats["table_path"])
