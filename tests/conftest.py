"""
Configurações globais e fixtures para a suite de testes pytest do QIMED.
"""
import os
import sys
import pytest
import pandas as pd
import yaml
import json
import tempfile

# Garante que a raiz do projeto esteja no PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Garante secrets de teste padrão para o pipeline e MPI
os.environ.setdefault("QIMED_MPI_SALT", "test_salt_secret_1234567890abcdef1234567890abcdef")
os.environ.setdefault("SALT_SECRET", "test_salt_secret_1234567890abcdef1234567890abcdef")


@pytest.fixture
def tmp_lakehouse(tmp_path):
    """Diretório temporário para gravações na camada Bronze."""
    lh = tmp_path / "lakehouse" / "bronze"
    lh.mkdir(parents=True)
    return str(lh)


@pytest.fixture
def sample_sih_df():
    """DataFrame de exemplo simulando microdados de internação do SIH/DATASUS."""
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
    """DataFrame de exemplo simulando dados cadastrais do CNES."""
    return pd.DataFrame({
        "CNES": ["1234567", "2345678", "3456789"],
        "CODUFMUN": ["230440", "230370", "231290"],
        "NOME_FANTASIA": ["Hospital A", "Clinica B", "Posto C"],
        "CPF_PROF": ["11111111111", "22222222222", "33333333333"],
        "NOME_PROF": ["Dr. Silva", "Dra. Santos", "Dr. Lima"],
    })


@pytest.fixture
def sample_fhir_patient_df():
    """DataFrame de exemplo simulando dados de Paciente FHIR achatados."""
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
    """Cria arquivo temporário do manifesto de governança PII/LGPD para testes."""
    manifest = {
        "datasus_sih": ["NASC", "N_AIH", "CPF_AUT", "NOME_PAC"],
        "datasus_cnes": ["CPF_PROF", "NOME_PROF"],
        "fhir_synthetic": ["name_family", "name_given", "birthDate", "cpf"],
    }
    manifest_file = tmp_path / "pii_manifest.yaml"
    with open(manifest_file, "w", encoding="utf-8") as f:
        yaml.dump(manifest, f)
    return str(manifest_file)
