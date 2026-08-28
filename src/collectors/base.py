"""
Classe base abstrata para coletores de dados do QIMED.
Implementa lógica de tentativas (retry), checkpointing e circuit breaker.
"""
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
    """Lançada quando o circuit breaker está aberto devido a falhas consecutivas."""
    pass


@dataclass
class CollectorConfig:
    """Configuração para um coletor de dados."""
    max_retries: int = 3
    retry_backoff: int = 5
    state_dir: str = ".collector_state"


class BaseCollector(abc.ABC):
    """
    Coletor base abstrato com lógica de repetição, checkpoint e disjuntor de circuito (circuit breaker).

    As subclasses implementam fetch(), parse() e get_source_type().
    O método run() orquestra o fluxo completo:
        fetch → parse → detecção PII → anonimização → validação → escrita na bronze → registro no catálogo
    """

    def __init__(self, config: CollectorConfig = None):
        self.config = config or CollectorConfig()
        self.consecutive_failures = 0

    @abc.abstractmethod
    def fetch(self) -> Any:
        """Coleta dados brutos da fonte. Retorna dados brutos (lista, bytes, etc.)."""
        pass

    @abc.abstractmethod
    def parse(self, raw_data: Any) -> pd.DataFrame:
        """Converte e estrutura os dados brutos em um DataFrame do pandas."""
        pass

    @abc.abstractmethod
    def get_source_type(self) -> str:
        """Retorna o identificador do tipo de fonte (ex.: 'datasus_sih')."""
        pass

    def save_checkpoint(self, state: Dict[str, Any]) -> None:
        """Salva o estado do checkpoint em disco para permitir ingestão retomável."""
        os.makedirs(self.config.state_dir, exist_ok=True)
        state_file = os.path.join(
            self.config.state_dir, f"{self.get_source_type()}_state.json"
        )
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
        logger.info(f"Checkpoint salvo: {state_file}")

    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Carrega o estado do checkpoint do disco, se existir."""
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
        Executa o pipeline completo de ingestão com retentativas e circuit breaker.

        Todas as dependências são injetadas para manter a classe base testável.
        """
        if self.consecutive_failures >= self.config.max_retries:
            raise CircuitBreakerOpen(
                f"Circuit breaker aberto após {self.consecutive_failures} falhas consecutivas."
            )

        attempt = 0
        while attempt < self.config.max_retries:
            try:
                logger.info(f"[{self.get_source_type()}] Tentativa {attempt + 1}/{self.config.max_retries}")

                # 1. Coleta
                raw_data = self.fetch()

                # 2. Parsing
                df = self.parse(raw_data)
                logger.info(f"Registros parseados: {len(df)}")

                # 3. Detecção de PII e Anonimização
                if pii_detector and anonymizer:
                    pii_fields = pii_detector.detect_pii_fields(
                        self.get_source_type(), df
                    )
                    if pii_fields:
                        logger.info(f"Campos PII detectados nas colunas: {pii_fields}")
                        df, audit_log = anonymizer.anonymize(df, pii_fields)
                        logger.info(f"Auditoria de anonimização: {audit_log}")

                # 4. Validação
                if validator:
                    result = validator.validate(df)
                    if not result.rejected_df.empty:
                        logger.warning(
                            f"Validação rejeitou {len(result.rejected_df)} linhas."
                        )
                    df = result.valid_df

                # 5. Escrita na Camada Bronze
                if bronze_writer:
                    metadata = {
                        "source": self.get_source_type().split("_")[0],
                        "subsystem": self.get_source_type(),
                        "source_type": self.get_source_type(),
                        "source_file": "collector_run",
                    }
                    write_stats = bronze_writer.write(df, metadata)
                    logger.info(f"Estatísticas de gravação Bronze: {write_stats}")

                # 6. Registro no Catálogo de Metadados
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
                logger.error(f"Tentativa {attempt} falhou: {e}")
                if attempt >= self.config.max_retries:
                    raise
                time.sleep(self.config.retry_backoff * attempt)

        return pd.DataFrame()
