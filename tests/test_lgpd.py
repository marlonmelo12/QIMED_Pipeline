"""Tests for the LGPD Gate: PIIDetector and Anonymizer."""
import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer


class TestPIIDetector:
    """Tests for PIIDetector."""

    def test_finds_known_pii_fields(self, pii_manifest_path, sample_sih_df):
        """PIIDetector should find PII fields that exist in both manifest and DataFrame."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("datasus_sih", sample_sih_df)

        # NASC and CPF_AUT are in both the manifest and the SIH DataFrame
        assert "NASC" in detected
        assert "CPF_AUT" in detected
        # N_AIH is in manifest and DataFrame
        assert "N_AIH" in detected

    def test_ignores_safe_fields(self, pii_manifest_path, sample_sih_df):
        """PIIDetector should NOT flag non-PII fields."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("datasus_sih", sample_sih_df)

        # These columns exist in the DF but are NOT in the PII manifest
        assert "DIAG_PRINC" not in detected
        assert "PROC_REA" not in detected
        assert "MUNIC_RES" not in detected

    def test_unknown_source_returns_empty(self, pii_manifest_path, sample_sih_df):
        """Unknown source_type should return an empty list."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("unknown_source", sample_sih_df)
        assert detected == []

    def test_detects_fhir_pii(self, pii_manifest_path, sample_fhir_patient_df):
        """PIIDetector should find PII fields in FHIR Patient data."""
        detector = PIIDetector(manifest_path=pii_manifest_path)
        detected = detector.detect_pii_fields("fhir_synthetic", sample_fhir_patient_df)

        assert "name_family" in detected
        assert "cpf" in detected
        assert "birthDate" in detected


class TestAnonymizer:
    """Tests for the Anonymizer."""

    def test_hashes_pii_fields(self, sample_sih_df):
        """Anonymizer should replace PII values with SHA-256 hashes."""
        anon = Anonymizer(salt="test_salt_123")
        pii_fields = ["NASC", "CPF_AUT"]

        result_df, audit_log = anon.anonymize(sample_sih_df, pii_fields)

        # Original values should be gone
        assert result_df["NASC"].iloc[0] != "19900101"
        assert result_df["CPF_AUT"].iloc[0] != "11111111111"

        # Values should be hex strings (SHA-256 = 64 hex chars)
        assert len(result_df["NASC"].iloc[0]) == 64
        assert all(c in "0123456789abcdef" for c in result_df["NASC"].iloc[0])

        # Audit log should record what was anonymized
        assert audit_log["status"] == "success"
        assert "NASC" in audit_log["anonymized_columns"]
        assert "CPF_AUT" in audit_log["anonymized_columns"]

    def test_is_deterministic(self, sample_sih_df):
        """Same input + same salt should produce the same hash every time."""
        salt = "deterministic_salt"
        anon = Anonymizer(salt=salt)

        result1, _ = anon.anonymize(sample_sih_df.copy(), ["CPF_AUT"])
        result2, _ = anon.anonymize(sample_sih_df.copy(), ["CPF_AUT"])

        assert result1["CPF_AUT"].tolist() == result2["CPF_AUT"].tolist()

    def test_different_salt_produces_different_hashes(self, sample_sih_df):
        """Different salts should produce different hashes for the same input."""
        anon1 = Anonymizer(salt="salt_alpha")
        anon2 = Anonymizer(salt="salt_beta")

        result1, _ = anon1.anonymize(sample_sih_df.copy(), ["CPF_AUT"])
        result2, _ = anon2.anonymize(sample_sih_df.copy(), ["CPF_AUT"])

        assert result1["CPF_AUT"].tolist() != result2["CPF_AUT"].tolist()

    def test_non_pii_fields_unchanged(self, sample_sih_df):
        """Fields NOT listed as PII should remain untouched."""
        anon = Anonymizer(salt="test_salt")
        original_diag = sample_sih_df["DIAG_PRINC"].tolist()

        result_df, _ = anon.anonymize(sample_sih_df, ["CPF_AUT"])

        assert result_df["DIAG_PRINC"].tolist() == original_diag

    def test_no_pii_fields_returns_unchanged(self, sample_sih_df):
        """Passing empty PII list should return the DataFrame unchanged."""
        anon = Anonymizer(salt="test_salt")
        result_df, audit_log = anon.anonymize(sample_sih_df, [])

        assert audit_log["status"] == "no_pii_fields_provided"
        pd.testing.assert_frame_equal(result_df, sample_sih_df)
