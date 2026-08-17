"""
Testes Unitarios para os Coletores e Validadores do SISREG / CROSS e ANS / D-TISS com LGPD Gate.
"""
import pytest
import pandas as pd
from src.collectors.sisreg_collector import SisregCollector
from src.collectors.ans_collector import AnsCollector
from src.validators.regulation_and_supplementary_validators import SisregValidator, AnsValidator
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer

def test_sisreg_collector_and_validator():
    collector = SisregCollector(uf="AC", year=2025, month=1)
    raw = collector.fetch()
    df = collector.parse(raw)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "ID_SOLICITACAO" in df.columns
    assert "DT_SOLICITACAO" in df.columns
    
    # Validacao
    validator = SisregValidator()
    result = validator.validate(df)
    assert len(result.valid_df) == len(df)
    assert len(result.rejected_df) == 0

def test_sisreg_lgpd_anonymization():
    collector = SisregCollector(uf="AC", year=2025, month=1)
    df = collector.parse(collector.fetch())
    
    detector = PIIDetector()
    anonymizer = Anonymizer()
    
    pii_fields = detector.detect_pii_fields("sisreg_regulation", df)
    assert len(pii_fields) > 0
    assert "CNS_PACIENTE" in pii_fields or "NM_PACIENTE" in pii_fields
    
    df_anon, metrics = anonymizer.anonymize(df, pii_fields)
    assert len(metrics["anonymized_columns"]) > 0
    # O valor original nao deve mais estar presente em texto claro
    if "NM_PACIENTE" in df_anon.columns:
        assert not df_anon["NM_PACIENTE"].str.startswith("PACIENTE REGULACAO 1").any()

def test_ans_collector_and_validator():
    collector = AnsCollector(uf="AC", year=2025)
    raw = collector.fetch()
    df = collector.parse(raw)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "CD_OPERADORA" in df.columns
    assert "NR_BENEFICIARIOS_ATIVOS" in df.columns
    
    # Validacao
    validator = AnsValidator()
    result = validator.validate(df)
    assert len(result.valid_df) == len(df)
    assert len(result.rejected_df) == 0
