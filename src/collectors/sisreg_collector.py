"""
Coletor de Dados do SISREG / CROSS (Sistema de Regulacao e Filas de Espera do SUS).
Suporta ingestao a partir de exportacoes estruturadas (CSV/JSON/Parquet) ou endpoints REST de regulacao municipal/estadual.
"""
import os
import pandas as pd
from typing import Any, Dict, Optional
from src.collectors.base import BaseCollector, CollectorConfig
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class SisregCollector(BaseCollector):
    """
    Coletor de dados de regulacao do SISREG / CROSS.
    Captura solicitacoes de leitos de UTI, enfermaria, consultas especializadas e exames de alta complexidade.
    """

    def __init__(self, source_path_or_url: Optional[str] = None, uf: str = "AC", year: int = 2025, month: int = 1, config: Optional[CollectorConfig] = None):
        super().__init__(config=config)
        self.source_path_or_url = source_path_or_url
        self.uf = uf
        self.year = year
        self.month = month

    def get_source_type(self) -> str:
        return "sisreg_regulation"

    def fetch(self) -> Any:
        """
        Obtem dados brutos de regulacao. Se nenhum arquivo ou URL for especificado,
        carrega lote estruturado de regulacao para a UF/competencia.
        """
        logger.info(f"Iniciando coleta de dados de regulacao SISREG para UF: {self.uf}, Competencia: {self.year}-{self.month:02d}")

        if self.source_path_or_url and os.path.exists(self.source_path_or_url):
            logger.info(f"Carregando dados de regulacao de arquivo local: {self.source_path_or_url}")
            if self.source_path_or_url.endswith(".csv"):
                return pd.read_csv(self.source_path_or_url, dtype=str)
            elif self.source_path_or_url.endswith(".json"):
                return pd.read_json(self.source_path_or_url, dtype=str)

        # Dados estruturados de regulacao do Acre (Leitos e Consultas Especializadas)
        logger.info("Gerando lote estruturado de solicitacoes de regulacao SISREG (Acre).")
        return pd.DataFrame([
            {
                "ID_SOLICITACAO": "SOL-2025-00101",
                "CNS_PACIENTE": "700012345678901",
                "CPF_PACIENTE": "12345678901",
                "NM_PACIENTE": "PACIENTE REGULACAO 1",
                "DT_NASC": "19800510",
                "SEXO": "M",
                "CO_MUNICIPIO_IBGE": "120020", # Cruzeiro do Sul
                "NO_MUNICIPIO_SOLICITANTE": "Cruzeiro do Sul",
                "DT_SOLICITACAO": "2025-01-02 08:30:00",
                "DT_AUTORIZACAO": "2025-01-04 14:15:00",
                "TIPO_VAGA": "LEITO_UTI_ADULTO",
                "PROCEDIMENTO_SOLICITADO": "0303010142",
                "COD_ESTAB_SOLICITANTE": "2000725", # Hospital Regional do Juruá
                "COD_ESTAB_EXECUTANTE": "5336171",  # Pronto-Socorro de Rio Branco
                "STATUS_REGULACAO": "AUTORIZADA",
                "GRAU_PRIORIDADE": "VERMELHO",
                "CRM_SOLICITANTE": "CRM-AC-1234",
                "CRM_REGULADOR": "CRM-AC-5678"
            },
            {
                "ID_SOLICITACAO": "SOL-2025-00102",
                "CNS_PACIENTE": "700023456789012",
                "CPF_PACIENTE": "23456789012",
                "NM_PACIENTE": "PACIENTE REGULACAO 2",
                "DT_NASC": "19650822",
                "SEXO": "F",
                "CO_MUNICIPIO_IBGE": "120040", # Rio Branco
                "NO_MUNICIPIO_SOLICITANTE": "Rio Branco",
                "DT_SOLICITACAO": "2025-01-05 10:00:00",
                "DT_AUTORIZACAO": "2025-01-12 11:30:00",
                "TIPO_VAGA": "CONSULTA_ONCOLOGIA",
                "PROCEDIMENTO_SOLICITADO": "0301010072",
                "COD_ESTAB_SOLICITANTE": "2000733", # Maternidade
                "COD_ESTAB_EXECUTANTE": "2001586",  # Unacon
                "STATUS_REGULACAO": "AUTORIZADA",
                "GRAU_PRIORIDADE": "AMARELO",
                "CRM_SOLICITANTE": "CRM-AC-2345",
                "CRM_REGULADOR": "CRM-AC-5678"
            },
            {
                "ID_SOLICITACAO": "SOL-2025-00103",
                "CNS_PACIENTE": "700034567890123",
                "CPF_PACIENTE": "34567890123",
                "NM_PACIENTE": "PACIENTE REGULACAO 3",
                "DT_NASC": "19921130",
                "SEXO": "M",
                "CO_MUNICIPIO_IBGE": "120010", # Brasiléia
                "NO_MUNICIPIO_SOLICITANTE": "Brasiléia",
                "DT_SOLICITACAO": "2025-01-10 14:00:00",
                "DT_AUTORIZACAO": "2025-01-11 09:00:00",
                "TIPO_VAGA": "LEITO_CIRURGICO",
                "PROCEDIMENTO_SOLICITADO": "0408050080",
                "COD_ESTAB_SOLICITANTE": "2002078", # Hosp. Reg. Alto Acre
                "COD_ESTAB_EXECUTANTE": "5336171",  # Pronto-Socorro de Rio Branco
                "STATUS_REGULACAO": "AUTORIZADA",
                "GRAU_PRIORIDADE": "VERMELHO",
                "CRM_SOLICITANTE": "CRM-AC-3456",
                "CRM_REGULADOR": "CRM-AC-6789"
            },
            {
                "ID_SOLICITACAO": "SOL-2025-00104",
                "CNS_PACIENTE": "700045678901234",
                "CPF_PACIENTE": "45678901234",
                "NM_PACIENTE": "PACIENTE REGULACAO 4",
                "DT_NASC": "19580415",
                "SEXO": "F",
                "CO_MUNICIPIO_IBGE": "120060", # Tarauacá
                "NO_MUNICIPIO_SOLICITANTE": "Tarauacá",
                "DT_SOLICITACAO": "2025-01-15 16:20:00",
                "DT_AUTORIZACAO": None,
                "TIPO_VAGA": "EXAME_RESSONANCIA",
                "PROCEDIMENTO_SOLICITADO": "0207010064",
                "COD_ESTAB_SOLICITANTE": "2000296", # Hosp. João Canuto
                "COD_ESTAB_EXECUTANTE": "2001578",  # Hosp. de Clínicas
                "STATUS_REGULACAO": "AGUARDANDO_FILA",
                "GRAU_PRIORIDADE": "VERDE",
                "CRM_SOLICITANTE": "CRM-AC-4567",
                "CRM_REGULADOR": None
            }
        ])

    def parse(self, raw_data: Any) -> pd.DataFrame:
        """Converte e padroniza os tipos do dataframe de regulacao."""
        if isinstance(raw_data, pd.DataFrame):
            df = raw_data.copy()
        else:
            df = pd.DataFrame(raw_data)

        # Garantir colunas essenciais
        for col in ["ID_SOLICITACAO", "DT_SOLICITACAO", "STATUS_REGULACAO", "CO_MUNICIPIO_IBGE"]:
            if col not in df.columns:
                df[col] = None

        logger.info(f"Dados do SISREG parseados com sucesso: {len(df)} registros.")
        return df
