"""
Testes unitarios para coletores, validadores e LGPD Gate do SIA (Ambulatorial).
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


def test_datasus_validator_sia():
    validator = DatasusValidator(subsystem="SIA")
    
    # Valid dataframe with PA_PROC_ID
    df_valid = pd.DataFrame([{
        "PA_CODUNI": "2000733",
        "PA_PROC_ID": "0301010072",
        "PA_QTDPRO": 1
    }])
    result = validator.validate(df_valid)
    assert len(result.valid_df) == 1
    assert len(result.rejected_df) == 0

    # Valid dataframe with APAC AP_PRID
    df_apac = pd.DataFrame([{
        "AP_PRID": "0303010010",
        "AP_AUTORIZ": "123456"
    }])
    result_apac = validator.validate(df_apac)
    assert len(result_apac.valid_df) == 1
    assert len(result_apac.rejected_df) == 0


def test_lgpd_gate_sia():
    detector = PIIDetector()
    anonymizer = Anonymizer(salt="test_salt_sia_123")

    df_sia = pd.DataFrame([{
        "PA_PROC_ID": "0301010072",
        "PA_AUTORIZ": "1234567890123",
        "PA_CNSMED": "123456789012345",
        "PA_NASC": "19850412",
        "PA_CODUNI": "2000733"
    }])

    pii_cols = detector.detect_pii_fields("datasus_sia", df_sia)
    assert "PA_AUTORIZ" in pii_cols
    assert "PA_CNSMED" in pii_cols
    assert "PA_NASC" in pii_cols

    df_anon, _ = anonymizer.anonymize(df_sia, pii_cols)
    assert df_anon["PA_AUTORIZ"].iloc[0] != "1234567890123"
    assert len(df_anon["PA_AUTORIZ"].iloc[0]) == 64
    assert len(df_anon["PA_NASC"].iloc[0]) == 64
    assert df_anon["PA_CODUNI"].iloc[0] == "2000733"  # Non-PII preserved


def test_sia_collector_integration():
    collector = DatasusCollector(subsystem="SIA", uf="AC", year=2026, month=1, sia_subgroup="PA")
    assert collector.get_remote_filename() == "PAAC2601.dbc"
    assert collector.get_source_type() == "datasus_sia"
