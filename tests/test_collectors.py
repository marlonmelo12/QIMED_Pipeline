"""Testes unitários para os coletores de dados."""
import os
import sys
import json
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.base import BaseCollector, CollectorConfig, CircuitBreakerOpen
from src.collectors.fhir_collector import FhirSyntheticCollector


# --- Coletor concreto para testes da classe base BaseCollector ---

class SuccessCollector(BaseCollector):
    """Coletor que sempre tem sucesso para testar a lógica da classe base."""

    def get_source_type(self) -> str:
        return "test_source"

    def fetch(self):
        return [{"id": "1", "value": "a"}, {"id": "2", "value": "b"}]

    def parse(self, raw_data) -> pd.DataFrame:
        return pd.DataFrame(raw_data)


class FailCollector(BaseCollector):
    """Coletor que sempre falha para testar retentativas e circuit breaker."""

    def get_source_type(self) -> str:
        return "fail_source"

    def fetch(self):
        raise ConnectionError("Falha simulada de conexão FTP")

    def parse(self, raw_data) -> pd.DataFrame:
        return pd.DataFrame()


class TestFhirSyntheticCollector:
    """Testes para o gerador de dados sintéticos FHIR."""

    def test_generates_valid_bundles(self):
        """O método fetch deve retornar dicionários de Bundles FHIR válidos."""
        collector = FhirSyntheticCollector(num_patients=5, encounters_per_patient=2)
        bundles = collector.fetch()

        assert len(bundles) == 5
        for bundle in bundles:
            assert bundle["resourceType"] == "Bundle"
            assert bundle["type"] == "collection"
            assert len(bundle["entry"]) > 0

            # O primeiro recurso deve ser um Patient
            first_resource = bundle["entry"][0]["resource"]
            assert first_resource["resourceType"] == "Patient"

    def test_respects_num_patients_config(self):
        """A quantidade de bundles gerados deve respeitar num_patients."""
        collector = FhirSyntheticCollector(num_patients=10, encounters_per_patient=1)
        bundles = collector.fetch()
        assert len(bundles) == 10

    def test_parse_produces_dataframe(self):
        """O método parse deve achatar bundles em um DataFrame com resourceType e id."""
        collector = FhirSyntheticCollector(num_patients=3, encounters_per_patient=2)
        bundles = collector.fetch()
        df = collector.parse(bundles)

        assert isinstance(df, pd.DataFrame)
        assert "resourceType" in df.columns
        assert "id" in df.columns
        assert len(df) > 0

        # Deve conter Patient, Encounter, Condition, Observation, Procedure, Organization
        resource_types = set(df["resourceType"].unique())
        assert "Patient" in resource_types
        assert "Encounter" in resource_types

    def test_patient_rows_have_flattened_pii(self):
        """Linhas de Patient devem ter campos PII achatados para detecção LGPD."""
        collector = FhirSyntheticCollector(num_patients=2, encounters_per_patient=1)
        bundles = collector.fetch()
        df = collector.parse(bundles)

        patients = df[df["resourceType"] == "Patient"]
        assert len(patients) == 2
        assert "name_family" in patients.columns
        assert "cpf" in patients.columns
        assert "birthDate" in patients.columns


class TestBaseCollectorCheckpoint:
    """Testes para o mecanismo de salvamento e recuperação de checkpoint."""

    def test_checkpoint_saves_state(self, tmp_path):
        """save_checkpoint deve gravar um arquivo de estado JSON."""
        config = CollectorConfig(state_dir=str(tmp_path / ".state"))
        collector = SuccessCollector(config=config)

        collector.save_checkpoint({"last_file": "RDCE2601.dbc", "offset": 1024})

        state_file = os.path.join(config.state_dir, "test_source_state.json")
        assert os.path.exists(state_file)

        with open(state_file, "r") as f:
            state = json.load(f)
        assert state["last_file"] == "RDCE2601.dbc"
        assert state["offset"] == 1024

    def test_checkpoint_loads_state(self, tmp_path):
        """load_checkpoint deve retornar o estado salvo."""
        config = CollectorConfig(state_dir=str(tmp_path / ".state"))
        collector = SuccessCollector(config=config)

        collector.save_checkpoint({"cursor": "abc123"})
        loaded = collector.load_checkpoint()

        assert loaded is not None
        assert loaded["cursor"] == "abc123"

    def test_checkpoint_returns_none_if_missing(self, tmp_path):
        """load_checkpoint deve retornar None se não houver arquivo de estado."""
        config = CollectorConfig(state_dir=str(tmp_path / ".no_state"))
        collector = SuccessCollector(config=config)

        assert collector.load_checkpoint() is None


class TestBaseCollectorCircuitBreaker:
    """Testes para o mecanismo de disjuntor (circuit breaker)."""

    def test_circuit_breaker_opens_after_consecutive_failures(self):
        """Após falhas consecutivas superiores a max_retries, o circuit breaker deve abrir."""
        config = CollectorConfig(max_retries=2, retry_backoff=0)
        collector = FailCollector(config=config)

        # Primeira execução: falha max_retries vezes e lança ConnectionError
        with pytest.raises(ConnectionError):
            collector.run()

        assert collector.consecutive_failures == 2

        # Segunda execução: disjuntor já está aberto
        with pytest.raises(CircuitBreakerOpen):
            collector.run()

    def test_success_resets_circuit_breaker(self):
        """Uma execução bem-sucedida deve reiniciar o contador de falhas."""
        config = CollectorConfig(max_retries=3, retry_backoff=0)
        collector = SuccessCollector(config=config)

        # Simula falhas prévias
        collector.consecutive_failures = 2

        # Execução com sucesso deve zerar falhas
        result = collector.run()
        assert collector.consecutive_failures == 0
        assert len(result) == 2
