"""
Semantic Mapper for DATASUS SIH (Sistema de Informações Hospitalares - AIH).
Transforms raw SIH hospitalization records into canonical Silver FHIR R4-aligned entities.
"""
import uuid
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.terminology import TerminologyService
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _parse_sih_date(val: Any) -> Optional[str]:
    """Parse DATASUS YYYYMMDD date strings or timestamps into ISO-8601 (YYYY-MM-DD)."""
    if pd.isna(val) or val is None or str(val).strip() in ("", "0", "00000000", "nan", "None"):
        return None
    val_str = str(val).split(".")[0].strip()
    if len(val_str) == 8 and val_str.isdigit():
        try:
            return f"{val_str[:4]}-{val_str[4:6]}-{val_str[6:8]}"
        except Exception:
            return None
    return str(val_str)


def _map_gender(val: Any) -> str:
    """Normalize DATASUS sex codes (1/M -> male, 3/F/2 -> female, other -> unknown)."""
    if pd.isna(val) or val is None:
        return "unknown"
    val_str = str(val).strip().upper()
    if val_str in ("1", "M", "MASC", "MASCULINO"):
        return "male"
    elif val_str in ("2", "3", "F", "FEM", "FEMININO"):
        return "female"
    return "other"


def _map_encounter_class(car_int: Any) -> str:
    """Map DATASUS CAR_INT (Caráter da Internação) to FHIR Encounter class code."""
    # 01: Eletivo, 02: Urgência/Emergência, 03: Acidente, 04: Parto, etc.
    if pd.isna(car_int) or car_int is None:
        return "IMP"  # Inpatient by default for SIH
    val_str = str(car_int).strip()
    if val_str in ("02", "2", "03", "3"):
        return "EMER"
    elif val_str in ("01", "1"):
        return "AMB"
    return "IMP"


class SihSemanticMapper(BaseSemanticMapper):
    """
    Transforms SIH hospitalization records into:
    - dim_patients
    - fct_encounters
    - fct_conditions
    - fct_procedures
    """

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df.empty:
            return CanonicalDataset()

        logger.info(f"Mapping {len(df)} SIH rows to canonical Silver model")
        src_meta = source_metadata or {}
        source_file = src_meta.get("source_file", "datasus_sih")

        patients_list = []
        encounters_list = []
        conditions_list = []
        procedures_list = []

        for idx, row in df.iterrows():
            # 1. Identifiers
            raw_aih = str(row.get("N_AIH", "")).strip()
            # If N_AIH is empty, generate deterministic hash from row values
            if not raw_aih or raw_aih in ("nan", "None"):
                raw_aih = f"sih_gen_{idx}_{uuid.uuid4().hex[:8]}"

            encounter_id = f"enc_sih_{raw_aih}"

            # Patient Identifier resolution
            # Use NASC or CPF_AUT or N_AIH to link
            raw_nasc = str(row.get("NASC", "")).strip()
            raw_cpf = str(row.get("CPF_AUT", "")).strip()
            munic_res = str(row.get("MUNIC_RES", "")).strip()
            
            # Stable patient reference
            if raw_cpf and raw_cpf not in ("nan", "None", ""):
                patient_id = f"pat_{raw_cpf}"
            elif raw_nasc and raw_nasc not in ("nan", "None", "") and len(raw_nasc) >= 8:
                patient_id = f"pat_hash_{hashlib.sha256(f'{raw_nasc}_{munic_res}'.encode()).hexdigest()[:16]}"
            else:
                patient_id = f"pat_sih_{raw_aih}"

            # 2. Patient Demographics
            norm_ibge, ibge_meta = TerminologyService.normalize_ibge_municipality(munic_res)
            uf_abbr = ibge_meta.get("uf_abbreviation") if ibge_meta else None
            gender = _map_gender(row.get("SEXO", None))

            patients_list.append({
                "patient_id": patient_id,
                "gender": gender,
                "birth_date_hash": raw_nasc if len(raw_nasc) == 64 else None,
                "municipality_code": norm_ibge,
                "state": uf_abbr,
                "_source_system": "DATASUS_SIH",
                "_updated_at": datetime.utcnow().isoformat()
            })

            # 3. Encounter
            start_date = _parse_sih_date(row.get("DT_INTER"))
            end_date = _parse_sih_date(row.get("DT_SAIDA"))
            
            # Hospital CNES
            cnes_code = str(row.get("CNES", row.get("MUNIC_MOV", "UNKNOWN"))).strip()
            org_id = f"org_cnes_{cnes_code}" if cnes_code != "UNKNOWN" else "org_cnes_unknown"

            diag_princ = str(row.get("DIAG_PRINC", "")).strip()
            norm_cid, cid_meta = TerminologyService.normalize_cid10(diag_princ)

            proc_rea = str(row.get("PROC_REA", "")).strip()
            norm_sigtap, sigtap_meta = TerminologyService.normalize_sigtap(proc_rea)

            # Length of stay
            perm_days = row.get("DIAS_PERM", None)
            try:
                length_of_stay = int(float(perm_days)) if pd.notna(perm_days) else None
            except Exception:
                length_of_stay = None

            # Financial Cost
            val_tot = row.get("VAL_TOT", None)
            try:
                total_cost = float(str(val_tot).replace(",", ".")) if pd.notna(val_tot) else 0.0
            except Exception:
                total_cost = 0.0

            # Outcome / Death
            morte = str(row.get("MORTE", "0")).strip()
            discharge_disposition = "expired" if morte in ("1", "true", "True") else "discharged_alive"

            encounters_list.append({
                "encounter_id": encounter_id,
                "patient_id": patient_id,
                "organization_id": org_id,
                "encounter_class": _map_encounter_class(row.get("CAR_INT")),
                "status": "finished",
                "period_start": start_date,
                "period_end": end_date,
                "length_of_stay_days": length_of_stay,
                "primary_diagnosis_code": norm_cid,
                "primary_diagnosis_chapter": cid_meta.get("chapter") if cid_meta else None,
                "primary_procedure_code": norm_sigtap,
                "total_cost_brl": total_cost,
                "discharge_disposition": discharge_disposition,
                "_source_file": source_file,
                "_updated_at": datetime.utcnow().isoformat()
            })

            # 4. Conditions
            if norm_cid:
                conditions_list.append({
                    "condition_id": f"cond_{encounter_id}_pri",
                    "encounter_id": encounter_id,
                    "patient_id": patient_id,
                    "code_system": "http://hl7.org/fhir/sid/icd-10",
                    "code": norm_cid,
                    "chapter": cid_meta.get("chapter") if cid_meta else None,
                    "chapter_description": cid_meta.get("chapter_description") if cid_meta else None,
                    "diagnosis_rank": "primary",
                    "recorded_date": start_date,
                    "_updated_at": datetime.utcnow().isoformat()
                })

            diag_sec = str(row.get("DIAG_SECUN", row.get("DIAGSEC1", ""))).strip()
            norm_sec, sec_meta = TerminologyService.normalize_cid10(diag_sec)
            if norm_sec:
                conditions_list.append({
                    "condition_id": f"cond_{encounter_id}_sec",
                    "encounter_id": encounter_id,
                    "patient_id": patient_id,
                    "code_system": "http://hl7.org/fhir/sid/icd-10",
                    "code": norm_sec,
                    "chapter": sec_meta.get("chapter") if sec_meta else None,
                    "chapter_description": sec_meta.get("chapter_description") if sec_meta else None,
                    "diagnosis_rank": "secondary",
                    "recorded_date": start_date,
                    "_updated_at": datetime.utcnow().isoformat()
                })

            # 5. Procedures
            if norm_sigtap:
                procedures_list.append({
                    "procedure_id": f"proc_{encounter_id}_01",
                    "encounter_id": encounter_id,
                    "patient_id": patient_id,
                    "code_system": "http://sigtap.datasus.gov.br",
                    "code": norm_sigtap,
                    "formatted_code": sigtap_meta.get("formatted_code") if sigtap_meta else norm_sigtap,
                    "group_description": sigtap_meta.get("group_description") if sigtap_meta else None,
                    "status": "completed",
                    "performed_date": start_date,
                    "_updated_at": datetime.utcnow().isoformat()
                })

        dim_patients = pd.DataFrame(patients_list).drop_duplicates(subset=["patient_id"])
        fct_encounters = pd.DataFrame(encounters_list).drop_duplicates(subset=["encounter_id"])
        fct_conditions = pd.DataFrame(conditions_list).drop_duplicates(subset=["condition_id"])
        fct_procedures = pd.DataFrame(procedures_list).drop_duplicates(subset=["procedure_id"])

        canonical = CanonicalDataset(
            dim_patients=dim_patients,
            dim_organizations=pd.DataFrame(),
            fct_encounters=fct_encounters,
            fct_conditions=fct_conditions,
            fct_procedures=fct_procedures,
            metadata={"source": "datasus_sih", "row_count": len(df)}
        )
        logger.info(f"Mapped canonical entities: {canonical.summary()}")
        return canonical
