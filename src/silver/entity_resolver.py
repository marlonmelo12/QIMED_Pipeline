"""
Entity Resolution & Master Indexing for QIMED DataQore.
Reconciles patient and facility identities across different healthcare data sources (MPI/MFI)
using deterministic and hashed link keys compliant with LGPD.
"""
import hashlib
from typing import Dict, Any, Tuple
import pandas as pd

from src.silver.mappers.base_mapper import CanonicalDataset
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class EntityResolver:
    """
    Resolves Master Patient Index (MPI) and Master Facility Index (MFI) keys
    across heterogeneous datasets.
    """

    def __init__(self, salt: str = "qimed_mpi_salt_2026"):
        self.salt = salt

    def generate_master_patient_id(self, patient_id: str, birth_hash: str = None, munic_code: str = None) -> str:
        """
        Derives a stable, deterministic Master Patient ID (MPI).
        """
        if birth_hash and munic_code and len(str(birth_hash)) >= 16:
            key = f"{birth_hash}_{munic_code}_{self.salt}"
            return f"mpi_{hashlib.sha256(key.encode()).hexdigest()[:16]}"
        
        # Fallback to source patient id combined with salt
        key = f"{patient_id}_{self.salt}"
        return f"mpi_{hashlib.sha256(key.encode()).hexdigest()[:16]}"

    def resolve(self, dataset: CanonicalDataset) -> CanonicalDataset:
        """
        Applies MPI and MFI resolution to all canonical entities in the dataset.
        Adds 'patient_master_id' to dim_patients and all fact tables.
        """
        if dataset.dim_patients.empty and dataset.fct_encounters.empty:
            return dataset

        logger.info("Running Entity Resolution (MPI/MFI linkage)")

        # 1. Build Patient ID -> Master Patient ID mapping
        patient_map: Dict[str, str] = {}

        if not dataset.dim_patients.empty:
            pts = dataset.dim_patients.copy()
            master_ids = []
            for _, row in pts.iterrows():
                pid = str(row.get("patient_id", ""))
                bhash = str(row.get("birth_date_hash", ""))
                mcode = str(row.get("municipality_code", ""))
                mpid = self.generate_master_patient_id(pid, bhash, mcode)
                patient_map[pid] = mpid
                master_ids.append(mpid)
            pts["patient_master_id"] = master_ids
            dataset.dim_patients = pts

        # 2. Update Fact Encounters
        if not dataset.fct_encounters.empty:
            encs = dataset.fct_encounters.copy()
            encs["patient_master_id"] = encs["patient_id"].apply(
                lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
            )
            dataset.fct_encounters = encs

        # 3. Update Fact Conditions
        if not dataset.fct_conditions.empty:
            conds = dataset.fct_conditions.copy()
            conds["patient_master_id"] = conds["patient_id"].apply(
                lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
            )
            dataset.fct_conditions = conds

        # 4. Update Fact Procedures
        if not dataset.fct_procedures.empty:
            procs = dataset.fct_procedures.copy()
            procs["patient_master_id"] = procs["patient_id"].apply(
                lambda pid: patient_map.get(pid, self.generate_master_patient_id(pid))
            )
            dataset.fct_procedures = procs

        # 5. Organizations MFI
        if not dataset.dim_organizations.empty:
            orgs = dataset.dim_organizations.copy()
            orgs["organization_master_id"] = orgs["organization_id"]
            dataset.dim_organizations = orgs

        logger.info(f"Entity resolution complete. Linked {len(patient_map)} unique patient IDs to Master Index.")
        return dataset
