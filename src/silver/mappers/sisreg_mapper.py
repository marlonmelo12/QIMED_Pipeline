"""
Mapper Semantico para o SISREG / CROSS.
Transforma dados de solicitacoes de regulacao e filas em fct_referrals, dim_patients e dim_organizations.
"""
from typing import Dict, Any, Optional
import pandas as pd
import hashlib
import numpy as np

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.entity_resolver import EntityResolver
from src.silver.terminology_names import (
    resolver_nome_doenca,
    resolver_nome_procedimento,
    resolver_nome_municipio,
    resolver_nome_hospital
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class SisregMapper(BaseSemanticMapper):
    """Mapeia solicitacoes e filas de regulacao do SISREG para o modelo analitico Silver."""

    def __init__(self, resolver: Optional[EntityResolver] = None):
        self.resolver = resolver or EntityResolver()

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df is None or df.empty:
            return CanonicalDataset()

        pdf = df.copy()
        
        # 1. Resolver Master Patient Index (MPI)
        if "patient_master_id" not in pdf.columns:
            patient_ids = []
            for _, row in pdf.iterrows():
                b_hash = str(row.get("DT_NASC", ""))
                m_code = str(row.get("CO_MUNICIPIO_IBGE", ""))
                p_id = str(row.get("CNS_PACIENTE", row.get("CPF_PACIENTE", "")))
                pid = self.resolver.generate_master_patient_id(
                    patient_id=p_id,
                    birth_hash=b_hash,
                    munic_code=m_code
                )
                patient_ids.append(pid)
            pdf["patient_master_id"] = patient_ids

        # 2. Dimensao dim_patients
        dim_patients = pd.DataFrame({
            "patient_master_id": pdf["patient_master_id"],
            "gender": pdf["SEXO"].map({"M": "male", "F": "female"}).fillna("unknown"),
            "birth_date_hash": pdf["DT_NASC"].astype(str),
            "municipality_code": pdf["CO_MUNICIPIO_IBGE"].astype(str),
            "municipality_name": pdf["CO_MUNICIPIO_IBGE"].apply(resolver_nome_municipio),
            "uf": "AC",
            "source_type": "sisreg_regulation"
        }).drop_duplicates("patient_master_id")

        # 3. Calcular Tempo de Espera na Fila (wait_time_days)
        requested_dt = pd.to_datetime(pdf["DT_SOLICITACAO"], errors="coerce")
        authorized_dt = pd.to_datetime(pdf["DT_AUTORIZACAO"], errors="coerce")
        
        wait_days = (authorized_dt - requested_dt).dt.total_seconds() / 86400.0
        wait_days = wait_days.fillna(0.0).round(2)

        # 4. Fato fct_referrals (Encaminhamentos e Filas)
        fct_referrals = pd.DataFrame({
            "referral_id": "ref_" + pdf["ID_SOLICITACAO"].astype(str),
            "patient_master_id": pdf["patient_master_id"],
            "request_organization_id": "org_cnes_" + pdf["COD_ESTAB_SOLICITANTE"].astype(str),
            "request_hospital_name": pdf["COD_ESTAB_SOLICITANTE"].apply(resolver_nome_hospital),
            "executing_organization_id": "org_cnes_" + pdf["COD_ESTAB_EXECUTANTE"].astype(str),
            "executing_hospital_name": pdf["COD_ESTAB_EXECUTANTE"].apply(resolver_nome_hospital),
            "municipality_code": pdf["CO_MUNICIPIO_IBGE"].astype(str),
            "municipality_name": pdf["CO_MUNICIPIO_IBGE"].apply(resolver_nome_municipio),
            "referral_type": pdf["TIPO_VAGA"].astype(str),
            "requested_procedure_code": pdf["PROCEDIMENTO_SOLICITADO"].astype(str),
            "requested_procedure_name": pdf["PROCEDIMENTO_SOLICITADO"].apply(resolver_nome_procedimento),
            "priority_level": pdf["GRAU_PRIORIDADE"].astype(str),
            "status": pdf["STATUS_REGULACAO"].astype(str),
            "requested_at": pdf["DT_SOLICITACAO"].astype(str),
            "authorized_at": pdf["DT_AUTORIZACAO"].astype(str),
            "wait_time_days": wait_days
        })

        logger.info(f"SisregMapper gerou {len(fct_referrals)} solicitacoes de regulacao para fct_referrals.")
        return CanonicalDataset(
            dim_patients=dim_patients,
            fct_referrals=fct_referrals,
            metadata=source_metadata or {}
        )
