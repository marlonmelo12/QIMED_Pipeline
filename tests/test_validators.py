"""Tests for DATASUS and FHIR validators."""
import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.validators.datasus_validator import DatasusValidator
from src.validators.fhir_validator import FhirValidator


class TestDatasusValidator:
    """Tests for the DatasusValidator."""

    def test_accepts_valid_sih(self, sample_sih_df):
        """Valid SIH data with all required columns should pass validation."""
        validator = DatasusValidator(subsystem="SIH")
        result = validator.validate(sample_sih_df)

        assert len(result.valid_df) == len(sample_sih_df)
        assert len(result.rejected_df) == 0
        assert result.stats["valid_count"] == 5

    def test_rejects_missing_columns(self):
        """SIH data missing required columns should be fully rejected."""
        df = pd.DataFrame({
            "DIAG_PRINC": ["A00", "B15"],
            "CEP": ["60000", "61000"],
        })
        validator = DatasusValidator(subsystem="SIH")
        result = validator.validate(df)

        # All rows rejected because required cols (N_AIH, ANO_CMPT, etc.) are missing
        assert len(result.valid_df) == 0
        assert len(result.rejected_df) == 2
        assert "Missing columns" in str(result.stats.get("reason", ""))

    def test_accepts_valid_cnes(self, sample_cnes_df):
        """Valid CNES data with required columns should pass."""
        validator = DatasusValidator(subsystem="CNES")
        result = validator.validate(sample_cnes_df)

        assert len(result.valid_df) == len(sample_cnes_df)
        assert len(result.rejected_df) == 0

    def test_cnes_rejects_missing_columns(self):
        """CNES data missing CNES or CODUFMUN should be rejected."""
        df = pd.DataFrame({
            "NOME_FANTASIA": ["Hospital A"],
        })
        validator = DatasusValidator(subsystem="CNES")
        result = validator.validate(df)

        assert len(result.valid_df) == 0
        assert "Missing columns" in str(result.stats.get("reason", ""))


class TestFhirValidator:
    """Tests for the FhirValidator."""

    def test_accepts_valid_patient(self, sample_fhir_patient_df):
        """FHIR Patient data with resourceType and id should pass."""
        validator = FhirValidator()
        result = validator.validate(sample_fhir_patient_df)

        assert len(result.valid_df) == 2
        assert len(result.rejected_df) == 0

    def test_rejects_missing_fields(self):
        """FHIR data without resourceType or id columns should be fully rejected."""
        df = pd.DataFrame({
            "name": ["Silva", "Santos"],
            "gender": ["male", "female"],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == 0
        assert "Missing columns" in str(result.stats.get("reason", ""))

    def test_rejects_invalid_resource_types(self):
        """Resources with invalid resourceType should be rejected."""
        df = pd.DataFrame({
            "resourceType": ["Patient", "InvalidResource", "Encounter"],
            "id": ["p1", "x1", "e1"],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == 2  # Patient and Encounter
        assert len(result.rejected_df) == 1  # InvalidResource
        assert result.rejected_df.iloc[0]["resourceType"] == "InvalidResource"

    def test_accepts_all_valid_resource_types(self):
        """All standard FHIR resource types should be accepted."""
        valid_types = ["Patient", "Encounter", "Observation", "Condition", "Procedure", "Organization"]
        df = pd.DataFrame({
            "resourceType": valid_types,
            "id": [f"id_{i}" for i in range(len(valid_types))],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == len(valid_types)
        assert len(result.rejected_df) == 0
