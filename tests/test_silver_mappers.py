"""
Testes unitários para os mappers semânticos da Camada Silver (SIH, CNES, FHIR) e EntityResolver.
"""
import pytest
import pandas as pd

from src.silver.mappers.sih_mapper import SihSemanticMapper
from src.silver.mappers.cnes_mapper import CnesSemanticMapper
from src.silver.mappers.fhir_mapper import FhirSemanticMapper
from src.silver.entity_resolver import EntityResolver


class TestSilverMappers:
    """Testes para mapeamento de dados brutos e semiestruturados de saúde para a Camada Silver."""

    def test_sih_mapper(self, sample_sih_df):
        """O mapper do SIH deve extrair pacientes, encontros/internações, condições e procedimentos."""
        mapper = SihSemanticMapper()
        canonical = mapper.map_to_canonical(sample_sih_df)

        assert len(canonical.dim_patients) > 0
        assert len(canonical.fct_encounters) == len(sample_sih_df)
        assert len(canonical.fct_conditions) >= len(sample_sih_df)
        assert len(canonical.fct_procedures) == len(sample_sih_df)

        # Verificação de colunas esperadas
        assert "patient_id" in canonical.dim_patients.columns
        assert "state" in canonical.dim_patients.columns
        assert "total_cost_brl" in canonical.fct_encounters.columns
        assert "discharge_disposition" in canonical.fct_encounters.columns
        assert "chapter" in canonical.fct_conditions.columns
        assert "group_description" in canonical.fct_procedures.columns

    def test_cnes_mapper(self, sample_cnes_df):
        """O mapper do CNES deve extrair estabelecimentos de saúde com códigos de 7 dígitos."""
        mapper = CnesSemanticMapper()
        canonical = mapper.map_to_canonical(sample_cnes_df)

        assert len(canonical.dim_organizations) == len(sample_cnes_df)
        assert "organization_id" in canonical.dim_organizations.columns
        assert "cnes_code" in canonical.dim_organizations.columns
        assert "state" in canonical.dim_organizations.columns
        assert canonical.dim_organizations["cnes_code"].iloc[0] == "1234567"

    def test_fhir_mapper(self):
        """O mapper FHIR deve normalizar linhas heterogêneas de bundles."""
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

        # Verificação de integridade referencial
        assert canonical.fct_encounters["patient_id"].iloc[0] == "pat_fhir_p1"
        assert canonical.fct_encounters["organization_id"].iloc[0] == "org_fhir_org1"

    def test_entity_resolver_mpi(self, sample_sih_df):
        """O EntityResolver deve enriquecer o dataset canônico com patient_master_id."""
        mapper = SihSemanticMapper()
        canonical = mapper.map_to_canonical(sample_sih_df)

        resolver = EntityResolver()
        resolved = resolver.resolve(canonical)

        assert "patient_master_id" in resolved.dim_patients.columns
        assert "patient_master_id" in resolved.fct_encounters.columns
        assert "patient_master_id" in resolved.fct_conditions.columns
        assert "patient_master_id" in resolved.fct_procedures.columns

        # Verifica prefixo do MPI
        assert resolved.dim_patients["patient_master_id"].iloc[0].startswith("mpi_")

    def test_sih_mapper_deterministic_fallback_keys(self):
        """Reprocessamento sem N_AIH deve gerar os mesmos encounter_id de forma determinística."""
        df_no_aih = pd.DataFrame([{
            "N_AIH": None,
            "CNES": "2000733",
            "DT_INTER": "2026-05-01",
            "DT_SAIDA": "2026-05-05",
            "MUNIC_RES": "120040",
            "PROC_REA": "0303010037",
            "VAL_TOT": "1500.50",
            "SEXO": "1",
            "DIAG_PRINC": "I10",
        }])

        mapper = SihSemanticMapper()
        res1 = mapper.map_to_canonical(df_no_aih)
        res2 = mapper.map_to_canonical(df_no_aih)

        enc1 = res1.fct_encounters["encounter_id"].iloc[0]
        enc2 = res2.fct_encounters["encounter_id"].iloc[0]

        assert enc1 == enc2
        assert enc1.startswith("enc_sih_sih_gen_")
