"""Tests for the data collectors."""
import os
import sys
import json
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.base import BaseCollector, CollectorConfig, CircuitBreakerOpen
from src.collectors.fhir_collector import FhirSyntheticCollector


# --- Concrete test collector for testing BaseCollector ---

class SuccessCollector(BaseCollector):
    """Always-succeeds collector for testing base class logic."""

    def get_source_type(self) -> str:
        return "test_source"

    def fetch(self):
        return [{"id": "1", "value": "a"}, {"id": "2", "value": "b"}]

    def parse(self, raw_data) -> pd.DataFrame:
        return pd.DataFrame(raw_data)


class FailCollector(BaseCollector):
    """Always-fails collector for testing retry and circuit breaker."""

    def get_source_type(self) -> str:
        return "fail_source"

    def fetch(self):
        raise ConnectionError("Simulated FTP failure")

    def parse(self, raw_data) -> pd.DataFrame:
        return pd.DataFrame()


class TestFhirSyntheticCollector:
    """Tests for the FHIR synthetic data generator."""

    def test_generates_valid_bundles(self):
        """Fetch should return FHIR Bundle dicts with entries."""
        collector = FhirSyntheticCollector(num_patients=5, encounters_per_patient=2)
        bundles = collector.fetch()

        assert len(bundles) == 5
        for bundle in bundles:
            assert bundle["resourceType"] == "Bundle"
            assert bundle["type"] == "collection"
            assert len(bundle["entry"]) > 0

            # First entry should be a Patient
            first_resource = bundle["entry"][0]["resource"]
            assert first_resource["resourceType"] == "Patient"

    def test_respects_num_patients_config(self):
        """Number of bundles should match num_patients."""
        collector = FhirSyntheticCollector(num_patients=10, encounters_per_patient=1)
        bundles = collector.fetch()
        assert len(bundles) == 10

    def test_parse_produces_dataframe(self):
        """Parse should flatten bundles into a DataFrame with resourceType and id."""
        collector = FhirSyntheticCollector(num_patients=3, encounters_per_patient=2)
        bundles = collector.fetch()
        df = collector.parse(bundles)

        assert isinstance(df, pd.DataFrame)
        assert "resourceType" in df.columns
        assert "id" in df.columns
        assert len(df) > 0

        # Should have Patient, Encounter, Condition, Observation, Procedure, Organization
        resource_types = set(df["resourceType"].unique())
        assert "Patient" in resource_types
        assert "Encounter" in resource_types

    def test_patient_rows_have_flattened_pii(self):
        """Patient rows should have flattened PII fields for LGPD detection."""
        collector = FhirSyntheticCollector(num_patients=2, encounters_per_patient=1)
        bundles = collector.fetch()
        df = collector.parse(bundles)

        patients = df[df["resourceType"] == "Patient"]
        assert len(patients) == 2
        assert "name_family" in patients.columns
        assert "cpf" in patients.columns
        assert "birthDate" in patients.columns


class TestBaseCollectorCheckpoint:
    """Tests for the checkpoint save/load mechanism."""

    def test_checkpoint_saves_state(self, tmp_path):
        """save_checkpoint should write a JSON state file."""
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
        """load_checkpoint should return saved state."""
        config = CollectorConfig(state_dir=str(tmp_path / ".state"))
        collector = SuccessCollector(config=config)

        collector.save_checkpoint({"cursor": "abc123"})
        loaded = collector.load_checkpoint()

        assert loaded is not None
        assert loaded["cursor"] == "abc123"

    def test_checkpoint_returns_none_if_missing(self, tmp_path):
        """load_checkpoint should return None if no state file exists."""
        config = CollectorConfig(state_dir=str(tmp_path / ".no_state"))
        collector = SuccessCollector(config=config)

        assert collector.load_checkpoint() is None


class TestBaseCollectorCircuitBreaker:
    """Tests for the circuit breaker mechanism."""

    def test_circuit_breaker_opens_after_consecutive_failures(self):
        """After max_retries consecutive failures, circuit breaker should open."""
        config = CollectorConfig(max_retries=2, retry_backoff=0)
        collector = FailCollector(config=config)

        # First run: fails max_retries times, raises ConnectionError
        with pytest.raises(ConnectionError):
            collector.run()

        assert collector.consecutive_failures == 2

        # Second run: circuit breaker is now open
        with pytest.raises(CircuitBreakerOpen):
            collector.run()

    def test_success_resets_circuit_breaker(self):
        """A successful run should reset the failure counter."""
        config = CollectorConfig(max_retries=3, retry_backoff=0)
        collector = SuccessCollector(config=config)

        # Simulate prior failures
        collector.consecutive_failures = 2

        # Successful run should reset
        result = collector.run()
        assert collector.consecutive_failures == 0
        assert len(result) == 2
