"""
Detector de Dados Pessoais Sensíveis (PII / LGPD) para o QIMED DataQore.
Identifica colunas com dados confidenciais baseando-se no manifesto de governança.
"""
import os
import yaml
from typing import List, Union, Dict, Any
import pandas as pd
import polars as pl

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class PIIDetector:
    """
    Componente do Portal LGPD (LGPD Gate) para detecção de Informações Pessoais Identificáveis (PII)
    com base em um manifesto de configuração YAML.
    """

    def __init__(self, manifest_path: str = None):
        """
        Inicializa o PIIDetector carregando o manifesto de PII.
        """
        if not manifest_path:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            manifest_path = os.path.join(base_dir, "config", "pii_manifest.yaml")
        
        self.manifest_path = manifest_path
        self.pii_mappings = self._load_manifest()

    def _load_manifest(self) -> Dict[str, List[str]]:
        """
        Carrega o manifesto YAML contendo os mapeamentos de PII por subsistema.
        """
        if not os.path.exists(self.manifest_path):
            logger.error(f"Manifesto PII não encontrado em {self.manifest_path}")
            return {}

        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                mappings = yaml.safe_load(f)
                logger.info(f"Manifesto PII carregado com {len(mappings)} tipos de fonte.")
                return mappings or {}
        except Exception as e:
            logger.error(f"Falha ao carregar manifesto PII: {e}")
            return {}

    def detect_pii_fields(self, source_type: str, data: Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]) -> List[str]:
        """
        Identifica quais colunas nos dados fornecidos contêm PII com base no manifesto.
        
        Argumentos:
            source_type: O tipo de fonte de dados (ex.: 'datasus_sih').
            data: O conjunto de dados (DataFrame ou Dicionário).
            
        Retorna:
            Lista de nomes de colunas/campos que contêm dados sensíveis PII.
        """
        if source_type not in self.pii_mappings:
            logger.warning(f"Tipo de fonte '{source_type}' não encontrado no manifesto PII.")
            return []

        known_pii_fields = set(self.pii_mappings[source_type])
        data_fields = set()

        if isinstance(data, pd.DataFrame):
            data_fields = set(data.columns)
        elif isinstance(data, pl.DataFrame):
            data_fields = set(data.columns)
        elif isinstance(data, dict):
            data_fields = set(data.keys())
        else:
            logger.error(f"Tipo de dado não suportado para detecção de PII: {type(data)}")
            return []

        # Interseção de campos presentes
        detected_fields = list(known_pii_fields.intersection(data_fields))
        
        if detected_fields:
            logger.info(f"Campos PII detectados para {source_type}: {detected_fields}")
        else:
            logger.info(f"Nenhum campo PII detectado para {source_type}.")
            
        return detected_fields
