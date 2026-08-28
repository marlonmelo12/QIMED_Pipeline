"""Testes unitários para o Portal LGPD: PIIDetector e Anonymizer."""
import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer


class TestPIIDetector:
    """Testes para o PIIDetector."""

    def test_finds_known_pii_fields(self, pii_manifest_path, sample_sih_df):
        """O PIIDetector deve encontrar campos PII existentes no manifesto e no DataFrame."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("datasus_sih", sample_sih_df)

        # NASC e CPF_AUT estão presentes no manifesto e no DataFrame do SIH
        assert "NASC" in detected
        assert "CPF_AUT" in detected
        assert "N_AIH" in detected

    def test_ignores_safe_fields(self, pii_manifest_path, sample_sih_df):
        """O PIIDetector NÃO deve marcar campos clínicos não sensíveis."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("datasus_sih", sample_sih_df)

        assert "DIAG_PRINC" not in detected
        assert "PROC_REA" not in detected
        assert "MUNIC_RES" not in detected

    def test_unknown_source_returns_empty(self, pii_manifest_path, sample_sih_df):
        """Fonte desconhecida deve retornar lista vazia."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("unknown_source", sample_sih_df)
        assert detected == []

    def test_detects_fhir_pii(self, pii_manifest_path, sample_fhir_patient_df):
        """O PIIDetector deve encontrar campos PII em dados de Patient do FHIR."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("fhir_synthetic", sample_fhir_patient_df)

        assert "name_family" in detected
        assert "cpf" in detected
        assert "birthDate" in detected


class TestAnonymizer:
    """Testes para o Anonymizer."""

    def test_hashes_pii_fields(self, sample_sih_df):
        """O Anonymizer deve substituir valores de PII por hashes SHA-256."""
        anon = Anonymizer(salt="test_salt_123")
        pii_fields = ["NASC", "CPF_AUT"]

        result_df, audit_log = anon.anonymize(sample_sih_df, pii_fields)

        # Valores originais não devem mais existir
        assert result_df["NASC"].iloc[0] != "19900101"
        assert result_df["CPF_AUT"].iloc[0] != "11111111111"

        # Valores devem ser strings hexadecimais (SHA-256 = 64 caracteres)
        assert len(result_df["NASC"].iloc[0]) == 64
        assert all(c in "0123456789abcdef" for c in result_df["NASC"].iloc[0])

        # O log de auditoria deve registrar os campos anonimizados
        assert audit_log["status"] == "success"
        assert "NASC" in audit_log["anonymized_columns"]
        assert "CPF_AUT" in audit_log["anonymized_columns"]

    def test_is_deterministic(self, sample_sih_df):
        """A mesma entrada com o mesmo salt deve produzir o mesmo hash."""
        salt = "deterministic_salt"
        anon = Anonymizer(salt=salt)

        result1, _ = anon.anonymize(sample_sih_df.copy(), ["CPF_AUT"])
        result2, _ = anon.anonymize(sample_sih_df.copy(), ["CPF_AUT"])

        assert result1["CPF_AUT"].tolist() == result2["CPF_AUT"].tolist()

    def test_different_salt_produces_different_hashes(self, sample_sih_df):
        """Salts diferentes devem produzir hashes diferentes para a mesma entrada."""
        anon1 = Anonymizer(salt="salt_alpha")
        anon2 = Anonymizer(salt="salt_beta")

        result1, _ = anon1.anonymize(sample_sih_df.copy(), ["CPF_AUT"])
        result2, _ = anon2.anonymize(sample_sih_df.copy(), ["CPF_AUT"])

        assert result1["CPF_AUT"].tolist() != result2["CPF_AUT"].tolist()

    def test_non_pii_fields_unchanged(self, sample_sih_df):
        """Campos que não são PII devem permanecer inalterados."""
        anon = Anonymizer(salt="test_salt")
        original_diag = sample_sih_df["DIAG_PRINC"].tolist()

        result_df, _ = anon.anonymize(sample_sih_df, ["CPF_AUT"])

        assert result_df["DIAG_PRINC"].tolist() == original_diag

    def test_no_pii_fields_returns_unchanged(self, sample_sih_df):
        """Passar lista vazia de PII deve retornar o DataFrame inalterado."""
        anon = Anonymizer(salt="test_salt")
        result_df, audit_log = anon.anonymize(sample_sih_df, [])

        assert audit_log["status"] == "no_pii_fields_provided"
        pd.testing.assert_frame_equal(result_df, sample_sih_df)
