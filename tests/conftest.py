import os
import sys
import pytest
import pandas as pd
import yaml
import json
import tempfile

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def tmp_lakehouse(tmp_path):
    """Temporary directory for Bronze writes."""
    lh = tmp_path / "lakehouse" / "bronze"
    lh.mkdir(parents=True)
    return str(lh)


@pytest.fixture
def sample_sih_df():
    """Small pandas DataFrame mimicking SIH data."""
    return pd.DataFrame({
        "N_AIH": ["1234567890123", "2345678901234", "3456789012345", "4567890123456", "5678901234567"],
        "ANO_CMPT": ["2026", "2026", "2026", "2026", "2026"],
        "MES_CMPT": ["01", "02", "03", "04", "05"],
        "PROC_REA": ["0301010010", "0301010028", "0301010036", "0301010044", "0301010052"],
        "CEP": ["60000000", "61000000", "62000000", "63000000", "64000000"],
        "MUNIC_RES": ["230440", "230370", "231290", "230760", "231130"],
        "NASC": ["19900101", "19850515", "19701231", "20001020", "20100305"],
        "DIAG_PRINC": ["A00", "B15", "C34", "D50", "E10"],
        "CPF_AUT": ["11111111111", "22222222222", "33333333333", "44444444444", "55555555555"],
    })


@pytest.fixture
def sample_cnes_df():
    """Small pandas DataFrame mimicking CNES data."""
    return pd.DataFrame({
        "CNES": ["1234567", "2345678", "3456789"],
        "CODUFMUN": ["230440", "230370", "231290"],
        "NOME_FANTASIA": ["Hospital A", "Clinica B", "Posto C"],
        "CPF_PROF": ["11111111111", "22222222222", "33333333333"],
        "NOME_PROF": ["Dr. Silva", "Dra. Santos", "Dr. Lima"],
    })


@pytest.fixture
def sample_fhir_patient_df():
    """Small DataFrame mimicking flattened FHIR Patient data."""
    return pd.DataFrame({
        "resourceType": ["Patient", "Patient"],
        "id": ["pat1", "pat2"],
        "name_family": ["Silva", "Santos"],
        "name_given": ["Joao", "Maria"],
        "birthDate": ["1990-01-01", "1985-05-15"],
        "gender": ["male", "female"],
        "cpf": ["11111111111", "22222222222"],
    })


@pytest.fixture
def pii_manifest_path(tmp_path):
    """Creates a test PII manifest YAML file."""
    manifest = {
        "datasus_sih": ["NASC", "N_AIH", "CPF_AUT", "NOME_PAC"],
        "datasus_cnes": ["CPF_PROF", "NOME_PROF"],
        "fhir_synthetic": ["name_family", "name_given", "birthDate", "cpf"],
    }
    manifest_file = tmp_path / "pii_manifest.yaml"
    with open(manifest_file, "w", encoding="utf-8") as f:
        yaml.dump(manifest, f)
    return str(manifest_file)
