"""Testes unitários para os validadores do DATASUS e FHIR."""
import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.validators.datasus_validator import DatasusValidator
from src.validators.fhir_validator import FhirValidator


class TestDatasusValidator:
    """Testes para o DatasusValidator."""

    def test_accepts_valid_sih(self, sample_sih_df):
        """Dados válidos do SIH contendo todas as colunas obrigatórias devem passar na validação."""
        validator = DatasusValidator(subsystem="SIH")
        result = validator.validate(sample_sih_df)

        assert len(result.valid_df) == len(sample_sih_df)
        assert len(result.rejected_df) == 0
        assert result.stats["valid_count"] == 5

    def test_rejects_missing_columns(self):
        """Dados do SIH sem colunas obrigatórias devem ser rejeitados."""
        df = pd.DataFrame({
            "DIAG_PRINC": ["A00", "B15"],
            "CEP": ["60000", "61000"],
        })
        validator = DatasusValidator(subsystem="SIH")
        result = validator.validate(df)

        # Todas as linhas devem ser rejeitadas por falta de colunas chave (N_AIH, ANO_CMPT, etc.)
        assert len(result.valid_df) == 0
        assert len(result.rejected_df) == 2
        assert "Missing columns" in str(result.stats.get("reason", ""))

    def test_accepts_valid_cnes(self, sample_cnes_df):
        """Dados válidos do CNES com colunas obrigatórias devem passar."""
        validator = DatasusValidator(subsystem="CNES")
        result = validator.validate(sample_cnes_df)

        assert len(result.valid_df) == len(sample_cnes_df)
        assert len(result.rejected_df) == 0

    def test_cnes_rejects_missing_columns(self):
        """Dados do CNES sem CNES ou CODUFMUN devem ser rejeitados."""
        df = pd.DataFrame({
            "NOME_FANTASIA": ["Hospital A"],
        })
        validator = DatasusValidator(subsystem="CNES")
        result = validator.validate(df)

        assert len(result.valid_df) == 0
        assert "Missing columns" in str(result.stats.get("reason", ""))


class TestFhirValidator:
    """Testes para o FhirValidator."""

    def test_accepts_valid_patient(self, sample_fhir_patient_df):
        """Recursos Patient do FHIR com resourceType e id devem passar."""
        validator = FhirValidator()
        result = validator.validate(sample_fhir_patient_df)

        assert len(result.valid_df) == 2
        assert len(result.rejected_df) == 0

    def test_rejects_missing_fields(self):
        """Dados FHIR sem colunas resourceType ou id devem ser rejeitados."""
        df = pd.DataFrame({
            "name": ["Silva", "Santos"],
            "gender": ["male", "female"],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == 0
        assert "Missing columns" in str(result.stats.get("reason", ""))

    def test_rejects_invalid_resource_types(self):
        """Recursos com resourceType desconhecido devem ser rejeitados."""
        df = pd.DataFrame({
            "resourceType": ["Patient", "InvalidResource", "Encounter"],
            "id": ["p1", "x1", "e1"],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == 2  # Patient e Encounter
        assert len(result.rejected_df) == 1  # InvalidResource
        assert result.rejected_df.iloc[0]["resourceType"] == "InvalidResource"

    def test_accepts_all_valid_resource_types(self):
        """Todos os tipos padrão de recursos FHIR suportados devem ser aceitos."""
        valid_types = ["Patient", "Encounter", "Observation", "Condition", "Procedure", "Organization"]
        df = pd.DataFrame({
            "resourceType": valid_types,
            "id": [f"id_{i}" for i in range(len(valid_types))],
        })
        validator = FhirValidator()
        result = validator.validate(df)

        assert len(result.valid_df) == len(valid_types)
        assert len(result.rejected_df) == 0
