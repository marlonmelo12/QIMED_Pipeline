"""
Unit tests for the Health Terminology Service (CID-10, SIGTAP, IBGE).
"""
import pytest
from src.silver.terminology import TerminologyService, FHIR_SYSTEM_CID10, FHIR_SYSTEM_SIGTAP, FHIR_SYSTEM_IBGE


class TestTerminologyService:
    """Tests for TerminologyService functions."""

    def test_cid10_valid_normalization(self):
        """Test normalization and chapter detection for valid CID-10 codes."""
        code, meta = TerminologyService.normalize_cid10("a090")
        assert code == "A09.0"
        assert meta["valid"] is True
        assert meta["chapter"] == "I"
        assert "infecciosas" in meta["chapter_description"].lower()

        # Test another chapter
        code_i10, meta_i10 = TerminologyService.normalize_cid10("I10")
        assert code_i10 == "I10"
        assert meta_i10["valid"] is True
        assert meta_i10["chapter"] == "IX"
        assert "circulatório" in meta_i10["chapter_description"].lower()

    def test_cid10_invalid_code(self):
        """Invalid CID-10 code format should be flagged."""
        code, meta = TerminologyService.normalize_cid10("123")
        assert meta["valid"] is False

    def test_cid10_empty_none(self):
        """Empty or None CID-10 should return None."""
        code, meta = TerminologyService.normalize_cid10("")
        assert code is None
        assert meta is None

        code_none, meta_none = TerminologyService.normalize_cid10(None)
        assert code_none is None
        assert meta_none is None

    def test_sigtap_normalization(self):
        """Test SIGTAP 10-digit procedure normalization."""
        # 10 digits
        code, meta = TerminologyService.normalize_sigtap("0301010010")
        assert code == "0301010010"
        assert meta["valid"] is True
        assert meta["group_code"] == "03"
        assert "clínicos" in meta["group_description"].lower()
        assert meta["formatted_code"] == "03.01.01.001-0"

        # 9 digits (missing leading zero)
        code_pad, meta_pad = TerminologyService.normalize_sigtap("301010010")
        assert code_pad == "0301010010"
        assert meta_pad["valid"] is True

    def test_sigtap_invalid_code(self):
        """Invalid length should be flagged."""
        code, meta = TerminologyService.normalize_sigtap("123")
        assert meta["valid"] is False

    def test_ibge_municipality_normalization(self):
        """Test IBGE 6/7 digit code parsing and UF extraction."""
        # Fortaleza - CE (2304400 or 230440)
        code, meta = TerminologyService.normalize_ibge_municipality("2304400")
        assert code == "230440"
        assert meta["valid"] is True
        assert meta["uf_code"] == "23"
        assert meta["uf_abbreviation"] == "CE"

        # São Paulo - SP (355030)
        code_sp, meta_sp = TerminologyService.normalize_ibge_municipality("355030")
        assert code_sp == "355030"
        assert meta_sp["uf_abbreviation"] == "SP"

    def test_build_fhir_concept(self):
        """Test FHIR CodeableConcept structure generation."""
        concept = TerminologyService.build_fhir_concept(FHIR_SYSTEM_CID10, "A09.0", "Gastroenterite")
        assert concept["coding"][0]["system"] == FHIR_SYSTEM_CID10
        assert concept["coding"][0]["code"] == "A09.0"
        assert concept["text"] == "Gastroenterite"
