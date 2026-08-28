"""
Mapper Semântico para o DATASUS SIH (Sistema de Informações Hospitalares - AIH).
Transforma registros brutos de internações hospitalares em entidades canônicas alinhadas ao FHIR R4.
"""
import os
import uuid
import hashlib
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.terminology import TerminologyService
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _parse_sih_date(val: Any) -> Optional[str]:
    """Converte strings de datas no formato YYYYMMDD ou timestamps para ISO-8601 (YYYY-MM-DD)."""
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
    """Normaliza códigos de sexo do DATASUS (1/M -> male, 3/F/2 -> female, outro -> unknown)."""
    if pd.isna(val) or val is None:
        return "unknown"
    val_str = str(val).strip().upper()
    if val_str in ("1", "M", "MASC", "MASCULINO"):
        return "male"
    elif val_str in ("2", "3", "F", "FEM", "FEMININO"):
        return "female"
    return "other"


def _map_encounter_class(car_int: Any) -> str:
    """Mapeia o CAR_INT do DATASUS (Caráter da Internação) para o código de classe FHIR Encounter."""
    # 01: Eletivo, 02: Urgência/Emergência, 03: Acidente, 04: Parto, etc.
    if pd.isna(car_int) or car_int is None:
        return "IMP"  # Internação hospitalar por padrão para o SIH
    val_str = str(car_int).strip()
    if val_str in ("02", "2", "03", "3"):
        return "EMER"
    elif val_str in ("01", "1"):
        return "AMB"
    return "IMP"


class SihSemanticMapper(BaseSemanticMapper):
    """
    Transforma registros de internações do SIH em:
    - dim_patients
    - fct_encounters
    - fct_conditions
    - fct_procedures
    """

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df.empty:
            return CanonicalDataset()

        logger.info(f"Mapeando {len(df)} registros do SIH para o modelo canônico da Camada Silver")
        src_meta = source_metadata or {}
        source_file = src_meta.get("source_file", "datasus_sih")

        patients_list = []
        encounters_list = []
        conditions_list = []
        procedures_list = []

        for idx, row in df.iterrows():
            raw_aih = str(row.get("N_AIH", "")).strip()
            # Se N_AIH estiver vazio, gera hash determinístico a partir dos atributos clínicos da linha
            if not raw_aih or raw_aih in ("nan", "None"):
                key_payload = f"{row.get('CNES', '')}_{row.get('DT_INTER', '')}_{row.get('DT_SAIDA', '')}_{row.get('MUNIC_RES', '')}_{row.get('PROC_REA', '')}_{row.get('VAL_TOT', '')}"
                stable_hash = hashlib.sha256(key_payload.encode()).hexdigest()[:16]
                raw_aih = f"sih_gen_{stable_hash}"

            encounter_id = f"enc_sih_{raw_aih}"

            # Resolução de identificador de paciente (LGPD & MPI Canônico)
            raw_nasc = str(row.get("NASC", "")).strip()
            munic_res = str(row.get("MUNIC_RES", "")).strip()
            raw_sexo = str(row.get("SEXO", "")).strip()
            mpi_salt = os.environ.get("QIMED_MPI_SALT", "qimed_secret_mpi_salt_v3")
            
            # Referência estável do paciente sem expor dados do autorizador (CPF_AUT)
            if raw_nasc and raw_nasc not in ("nan", "None", "") and len(raw_nasc) >= 8:
                natural_key = f"{raw_nasc}|{munic_res}|{raw_sexo}"
                patient_id = "pat_hash_" + hashlib.sha256(f"{natural_key}|{mpi_salt}".encode()).hexdigest()[:32]
            else:
                patient_id = f"pat_sih_{raw_aih}"

            # 2. Dados demográficos do paciente
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

            # 3. Encontro / Internação (Encounter)
            start_date = _parse_sih_date(row.get("DT_INTER"))
            end_date = _parse_sih_date(row.get("DT_SAIDA"))
            
            # Hospital CNES
            cnes_code = str(row.get("CNES", row.get("MUNIC_MOV", "UNKNOWN"))).strip()
            org_id = f"org_cnes_{cnes_code}" if cnes_code != "UNKNOWN" else "org_cnes_unknown"

            diag_princ = str(row.get("DIAG_PRINC", "")).strip()
            norm_cid, cid_meta = TerminologyService.normalize_cid10(diag_princ)

            proc_rea = str(row.get("PROC_REA", "")).strip()
            norm_sigtap, sigtap_meta = TerminologyService.normalize_sigtap(proc_rea)

            # Dias de permanência
            perm_days = row.get("DIAS_PERM", None)
            try:
                length_of_stay = int(float(perm_days)) if pd.notna(perm_days) else None
            except Exception:
                length_of_stay = None

            # Custo financeiro (Decimal seguro)
            val_tot = row.get("VAL_TOT", None)
            try:
                total_cost = float(Decimal(str(val_tot).replace(",", "."))) if pd.notna(val_tot) and str(val_tot).strip() not in ("", "nan", "None") else None
            except (InvalidOperation, TypeError, ValueError):
                total_cost = None

            # Desfecho / Óbito
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

            # 4. Diagnósticos e Condições Clínicas (Conditions)
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

            # 5. Procedimentos Médicos (Procedures)
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
        logger.info(f"Entidades canônicas mapeadas: {canonical.summary()}")
        return canonical
