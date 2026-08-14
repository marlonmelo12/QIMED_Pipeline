"""
Unit tests for Silver semantic mappers (SIH, CNES, FHIR) and EntityResolver.
"""
import pytest
import pandas as pd

from src.silver.mappers.sih_mapper import SihSemanticMapper
from src.silver.mappers.cnes_mapper import CnesSemanticMapper
from src.silver.mappers.fhir_mapper import FhirSemanticMapper
from src.silver.entity_resolver import EntityResolver


class TestSilverMappers:
    """Tests for mapping raw and semi-structured health datasets to Silver."""

    def test_sih_mapper(self, sample_sih_df):
        """SIH mapper should extract patients, encounters, conditions, and procedures."""
        mapper = SihSemanticMapper()
        canonical = mapper.map_to_canonical(sample_sih_df)

        assert len(canonical.dim_patients) > 0
        assert len(canonical.fct_encounters) == len(sample_sih_df)
        assert len(canonical.fct_conditions) >= len(sample_sih_df)
        assert len(canonical.fct_procedures) == len(sample_sih_df)

        # Check column presence
        assert "patient_id" in canonical.dim_patients.columns
        assert "state" in canonical.dim_patients.columns
        assert "total_cost_brl" in canonical.fct_encounters.columns
        assert "discharge_disposition" in canonical.fct_encounters.columns
        assert "chapter" in canonical.fct_conditions.columns
        assert "group_description" in canonical.fct_procedures.columns

    def test_cnes_mapper(self, sample_cnes_df):
        """CNES mapper should extract organizations with clean CNES codes."""
        mapper = CnesSemanticMapper()
        canonical = mapper.map_to_canonical(sample_cnes_df)

        assert len(canonical.dim_organizations) == len(sample_cnes_df)
        assert "organization_id" in canonical.dim_organizations.columns
        assert "cnes_code" in canonical.dim_organizations.columns
        assert "state" in canonical.dim_organizations.columns
        assert canonical.dim_organizations["cnes_code"].iloc[0] == "1234567"

    def test_fhir_mapper(self):
        """FHIR mapper should normalize heterogeneous bundle rows."""
        raw_fhir_df = pd.DataFrame([
            {
                "resourceType": "Patient",
                "id": "p1",
                "gender": "male",
                "birthDate": "1990-01-01",
                "raw_json": '{"address": [{"city": "230440"}]}'
            },
            {
                "resourceType": "Organization",
                "id": "org1",
                "raw_json": '{"name": "Hospital Geral", "identifier": [{"system": "http://cnes.saude.gov.br", "value": "7654321"}]}'
            },
            {
                "resourceType": "Encounter",
                "id": "enc1",
                "raw_json": '{"subject": {"reference": "Patient/p1"}, "serviceProvider": {"reference": "Organization/org1"}, "period": {"start": "2026-01-01", "end": "2026-01-04"}}'
            },
            {
                "resourceType": "Condition",
                "id": "cond1",
                "raw_json": '{"subject": {"reference": "Patient/p1"}, "encounter": {"reference": "Encounter/enc1"}, "code": {"coding": [{"code": "J18"}]}}'
            },
            {
                "resourceType": "Procedure",
                "id": "proc1",
                "raw_json": '{"subject": {"reference": "Patient/p1"}, "encounter": {"reference": "Encounter/enc1"}, "code": {"coding": [{"code": "0301010010"}]}}'
            }
        ])

        mapper = FhirSemanticMapper()
        canonical = mapper.map_to_canonical(raw_fhir_df)

        assert len(canonical.dim_patients) == 1
        assert len(canonical.dim_organizations) == 1
        assert len(canonical.fct_encounters) == 1
        assert len(canonical.fct_conditions) == 1
        assert len(canonical.fct_procedures) == 1

        # Check references
        assert canonical.fct_encounters["patient_id"].iloc[0] == "pat_fhir_p1"
        assert canonical.fct_encounters["organization_id"].iloc[0] == "org_fhir_org1"

    def test_entity_resolver_mpi(self, sample_sih_df):
        """EntityResolver should enrich canonical dataset with patient_master_id."""
        mapper = SihSemanticMapper()
        canonical = mapper.map_to_canonical(sample_sih_df)

        resolver = EntityResolver()
        resolved = resolver.resolve(canonical)

        assert "patient_master_id" in resolved.dim_patients.columns
        assert "patient_master_id" in resolved.fct_encounters.columns
        assert "patient_master_id" in resolved.fct_conditions.columns
        assert "patient_master_id" in resolved.fct_procedures.columns

        # Verify MPI prefix
        assert resolved.dim_patients["patient_master_id"].iloc[0].startswith("mpi_")
