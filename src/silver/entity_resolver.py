"""
Resolução de Entidades e Índice Mestre de Pacientes (MPI/MFI) para o QIMED DataQore.
Reconcilia identidades de pacientes e estabelecimentos entre diferentes fontes de dados de saúde
utilizando chaves determinísticas e criptografia em conformidade com a LGPD.
"""
import hashlib
from typing import Dict, Any, Tuple
import pandas as pd

from src.silver.mappers.base_mapper import CanonicalDataset
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class EntityResolver:
    """
    Resolve chaves do Master Patient Index (MPI) e Master Facility Index (MFI)
    entre conjuntos de dados heterogêneos.
    """

    def __init__(self, salt: str = "qimed_mpi_salt_2026"):
        self.salt = salt

    def generate_master_patient_id(self, patient_id: str, birth_hash: str = None, munic_code: str = None) -> str:
        """
        Deriva um ID Mestre de Paciente (MPI) determinístico e estável.
        """
        if birth_hash and munic_code and len(str(birth_hash)) >= 16:
            key = f"{birth_hash}_{munic_code}_{self.salt}"
            return f"mpi_{hashlib.sha256(key.encode()).hexdigest()[:16]}"
        
        # Fallback combinando ID de origem com o salt
        key = f"{patient_id}_{self.salt}"
        return f"mpi_{hashlib.sha256(key.encode()).hexdigest()[:16]}"

    def resolve(self, dataset: CanonicalDataset) -> CanonicalDataset:
        """
        Aplica a resolução de MPI e MFI em todas as entidades canônicas do dataset.
        Adiciona 'patient_master_id' na dim_patients e em todas as tabelas fato.
        """
        if dataset.dim_patients.empty and dataset.fct_encounters.empty and dataset.fct_referrals.empty:
            return dataset

        logger.info("Executando Resolução de Entidades (linkage MPI/MFI)")

        # 1. Constrói mapa Patient ID -> Master Patient ID
        patient_map: Dict[str, str] = {}

        if not dataset.dim_patients.empty:
            pts = dataset.dim_patients.copy()
            master_ids = []
            for _, row in pts.iterrows():
                pid = str(row.get("patient_id", row.get("patient_master_id", "")))
                bhash = str(row.get("birth_date_hash", ""))
                mcode = str(row.get("municipality_code", ""))
                mpid = self.generate_master_patient_id(pid, bhash, mcode)
                patient_map[pid] = mpid
                master_ids.append(mpid)
            pts["patient_master_id"] = master_ids
            dataset.dim_patients = pts

        # 2. Atualiza Fato Internações (fct_encounters)
        if not dataset.fct_encounters.empty:
            encs = dataset.fct_encounters.copy()
            if "patient_id" in encs.columns:
                encs["patient_master_id"] = encs["patient_id"].apply(
                    lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
                )
            dataset.fct_encounters = encs

        # 3. Atualiza Fato Diagnósticos/Condições (fct_conditions)
        if not dataset.fct_conditions.empty:
            conds = dataset.fct_conditions.copy()
            if "patient_id" in conds.columns:
                conds["patient_master_id"] = conds["patient_id"].apply(
                    lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
                )
            dataset.fct_conditions = conds

        # 4. Atualiza Fato Procedimentos (fct_procedures)
        if not dataset.fct_procedures.empty:
            procs = dataset.fct_procedures.copy()
            if "patient_id" in procs.columns:
                procs["patient_master_id"] = procs["patient_id"].apply(
                    lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
                )
            dataset.fct_procedures = procs

        # 5. Atualiza Fato Regulação e Filas (fct_referrals)
        if not dataset.fct_referrals.empty:
            refs = dataset.fct_referrals.copy()
            if "patient_id" in refs.columns and "patient_master_id" not in refs.columns:
                refs["patient_master_id"] = refs["patient_id"].apply(
                    lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
                )
            dataset.fct_referrals = refs

        # 6. Estabelecimentos MFI (dim_organizations)
        if not dataset.dim_organizations.empty:
            orgs = dataset.dim_organizations.copy()
            orgs["organization_master_id"] = orgs["organization_id"]
            dataset.dim_organizations = orgs

        logger.info(f"Resolução de entidades concluída. Vinculados {len(patient_map)} pacientes únicos ao Master Index.")
        return dataset
