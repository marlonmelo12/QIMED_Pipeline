import abc
import os
import time
import json
import hashlib
from typing import Any, Dict, Optional
import pandas as pd
from dataclasses import dataclass, field

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class CircuitBreakerOpen(Exception):
    """Raised when the circuit breaker is open due to consecutive failures."""
    pass


@dataclass
class CollectorConfig:
    """Configuration for a data collector."""
    max_retries: int = 3
    retry_backoff: int = 5
    state_dir: str = ".collector_state"


class BaseCollector(abc.ABC):
    """
    Abstract base collector with retry logic, checkpointing, and circuit breaker.

    Subclasses implement fetch(), parse(), and get_source_type().
    The run() method orchestrates the full pipeline:
        fetch → parse → PII detect → anonymize → validate → write bronze → register catalog
    """

    def __init__(self, config: CollectorConfig = None):
        self.config = config or CollectorConfig()
        self.consecutive_failures = 0

    @abc.abstractmethod
    def fetch(self) -> Any:
        """Fetch raw data from the source. Returns raw data (list, bytes, etc.)."""
        pass

    @abc.abstractmethod
    def parse(self, raw_data: Any) -> pd.DataFrame:
        """Parse raw data into a pandas DataFrame."""
        pass

    @abc.abstractmethod
    def get_source_type(self) -> str:
        """Return the source type identifier (e.g., 'datasus_sih')."""
        pass

    def save_checkpoint(self, state: Dict[str, Any]) -> None:
        """Save checkpoint state to disk for resumable ingestion."""
        os.makedirs(self.config.state_dir, exist_ok=True)
        state_file = os.path.join(
            self.config.state_dir, f"{self.get_source_type()}_state.json"
        )
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
        logger.info(f"Checkpoint saved: {state_file}")

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load checkpoint state from disk, if it exists."""
        state_file = os.path.join(
            self.config.state_dir, f"{self.get_source_type()}_state.json"
        )
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def run(
        self,
        pii_detector=None,
        anonymizer=None,
        validator=None,
        bronze_writer=None,
        catalog=None,
    ) -> pd.DataFrame:
        """
        Execute the full ingestion pipeline with retry and circuit breaker.

        All dependencies are injected to keep the base class testable.
        """
        if self.consecutive_failures >= self.config.max_retries:
            raise CircuitBreakerOpen(
                f"Circuit breaker open after {self.consecutive_failures} consecutive failures."
            )

        attempt = 0
        while attempt < self.config.max_retries:
            try:
                logger.info(f"[{self.get_source_type()}] Attempt {attempt + 1}/{self.config.max_retries}")

                # 1. Fetch
                raw_data = self.fetch()

                # 2. Parse
                df = self.parse(raw_data)
                logger.info(f"Parsed {len(df)} rows")

                # 3. Detect PII & Anonymize
                if pii_detector and anonymizer:
                    pii_fields = pii_detector.detect_pii_fields(
                        self.get_source_type(), df
                    )
                    if pii_fields:
                        logger.info(f"PII detected in columns: {pii_fields}")
                        df, audit_log = anonymizer.anonymize(df, pii_fields)
                        logger.info(f"Anonymization audit: {audit_log}")

                # 4. Validate
                if validator:
                    result = validator.validate(df)
                    if not result.rejected_df.empty:
                        logger.warning(
                            f"Validation rejected {len(result.rejected_df)} rows."
                        )
                    df = result.valid_df

                # 5. Write to Bronze
                if bronze_writer:
                    metadata = {
                        "source": self.get_source_type().split("_")[0],
                        "subsystem": self.get_source_type(),
                        "source_type": self.get_source_type(),
                        "source_file": "collector_run",
                    }
                    write_stats = bronze_writer.write(df, metadata)
                    logger.info(f"Bronze write stats: {write_stats}")

                # 6. Register in catalog
                if catalog:
                    catalog.register_dataset(
                        source_type=self.get_source_type(),
                        partition_path=write_stats.get("table_path", "") if bronze_writer else "",
                        row_count=len(df),
                        schema_fingerprint=hashlib.md5(
                            str(list(df.columns)).encode()
                        ).hexdigest() if len(df) > 0 else "",
                        pii_anonymized=bool(pii_detector and anonymizer),
                    )

                self.consecutive_failures = 0
                return df

            except Exception as e:
                attempt += 1
                self.consecutive_failures += 1
                logger.error(f"Attempt {attempt} failed: {e}")
                if attempt >= self.config.max_retries:
                    raise
                time.sleep(self.config.retry_backoff * attempt)

        return pd.DataFrame()
