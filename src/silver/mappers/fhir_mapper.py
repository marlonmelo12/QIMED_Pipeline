"""
Semantic Mapper for Bronze FHIR Synthetic and Raw FHIR Bundles.
Normalizes heterogeneous FHIR R4 resources into clean Silver canonical tables.
"""
import ast
import json
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.terminology import TerminologyService
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _safe_parse_dict(val: Any) -> Dict[str, Any]:
    """Safely parse string representations of JSON/dict."""
    if isinstance(val, dict):
        return val
    if not val or pd.isna(val):
        return {}
    try:
        return json.loads(val)
    except Exception:
        try:
            return ast.literal_eval(str(val))
        except Exception:
            return {}


class FhirSemanticMapper(BaseSemanticMapper):
    """
    Normalizes tabular/raw Bronze FHIR resources into Silver canonical entities.
    """

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df.empty:
            return CanonicalDataset()

        logger.info(f"Normalizing {len(df)} Bronze FHIR resources to Silver model")

        patients_list = []
        organizations_list = []
        encounters_list = []
        conditions_list = []
        procedures_list = []

        # Split by resourceType
        resource_groups = {rt: group for rt, group in df.groupby("resourceType")}

        # 1. Patients
        if "Patient" in resource_groups:
            pats = resource_groups["Patient"]
            for _, row in pats.iterrows():
                pat_id = str(row.get("id", ""))
                parsed_raw = _safe_parse_dict(row.get("raw_json", {}))

                address = parsed_raw.get("address", [{}])
                city_code = address[0].get("city") if address else None
                norm_ibge, ibge_meta = TerminologyService.normalize_ibge_municipality(city_code)

                patients_list.append({
                    "patient_id": f"pat_fhir_{pat_id}",
                    "gender": str(row.get("gender", "unknown")),
                    "birth_date_hash": str(row.get("birthDate", "")),
                    "municipality_code": norm_ibge,
                    "state": ibge_meta.get("uf_abbreviation") if ibge_meta else None,
                    "_source_system": "FHIR_R4",
                    "_updated_at": datetime.utcnow().isoformat()
                })

        # 2. Organizations
        if "Organization" in resource_groups:
            orgs = resource_groups["Organization"]
            for _, row in orgs.iterrows():
                org_id = str(row.get("id", ""))
                parsed_raw = _safe_parse_dict(row.get("raw_json", {}))
                name = parsed_raw.get("name", f"Organization {org_id}")

                cnes_val = None
                for ident in parsed_raw.get("identifier", []):
                    if "cnes" in ident.get("system", "").lower():
                        cnes_val = ident.get("value")

                organizations_list.append({
                    "organization_id": f"org_fhir_{org_id}",
                    "cnes_code": cnes_val or org_id[:7],
                    "name": name,
                    "facility_type": "Hospital Geral",
                    "municipality_code": None,
                    "state": None,
                    "bed_capacity": 50,
                    "_source_file": "fhir_synthetic",
                    "_updated_at": datetime.utcnow().isoformat()
                })

        # 3. Encounters
        if "Encounter" in resource_groups:
            encs = resource_groups["Encounter"]
            for _, row in encs.iterrows():
                enc_id = str(row.get("id", ""))
                parsed_raw = _safe_parse_dict(row.get("raw_json", {}))

                subj_ref = row.get("subject_ref", "") or parsed_raw.get("subject", {}).get("reference", "")
                pat_ref_id = subj_ref.replace("Patient/", "") if subj_ref else "unknown"

                prov_ref = parsed_raw.get("serviceProvider", {}).get("reference", "")
                org_ref_id = prov_ref.replace("Organization/", "") if prov_ref else "unknown"

                period = parsed_raw.get("period", {})
                start_dt = period.get("start", row.get("period_start", None))
                end_dt = period.get("end", row.get("period_end", None))

                enc_class = parsed_raw.get("class", {}).get("code", "IMP")

                encounters_list.append({
                    "encounter_id": f"enc_fhir_{enc_id}",
                    "patient_id": f"pat_fhir_{pat_ref_id}",
                    "organization_id": f"org_fhir_{org_ref_id}",
                    "encounter_class": enc_class,
                    "status": parsed_raw.get("status", "finished"),
                    "period_start": start_dt[:10] if start_dt and len(start_dt) >= 10 else None,
                    "period_end": end_dt[:10] if end_dt and len(end_dt) >= 10 else None,
                    "length_of_stay_days": 3,
                    "primary_diagnosis_code": None,
                    "primary_diagnosis_chapter": None,
                    "primary_procedure_code": None,
                    "total_cost_brl": 1500.0,
                    "discharge_disposition": "discharged_alive",
                    "_source_file": "fhir_synthetic",
                    "_updated_at": datetime.utcnow().isoformat()
                })

        # 4. Conditions
        if "Condition" in resource_groups:
            conds = resource_groups["Condition"]
            for _, row in conds.iterrows():
                cond_id = str(row.get("id", ""))
                parsed_raw = _safe_parse_dict(row.get("raw_json", {}))

                subj_ref = parsed_raw.get("subject", {}).get("reference", "")
                pat_ref_id = subj_ref.replace("Patient/", "") if subj_ref else "unknown"

                enc_ref = parsed_raw.get("encounter", {}).get("reference", "")
                enc_ref_id = enc_ref.replace("Encounter/", "") if enc_ref else "unknown"

                codings = parsed_raw.get("code", {}).get("coding", [])
                code_val = codings[0].get("code", "R10") if codings else "R10"
                norm_cid, cid_meta = TerminologyService.normalize_cid10(code_val)

                conditions_list.append({
                    "condition_id": f"cond_fhir_{cond_id}",
                    "encounter_id": f"enc_fhir_{enc_ref_id}",
                    "patient_id": f"pat_fhir_{pat_ref_id}",
                    "code_system": "http://hl7.org/fhir/sid/icd-10",
                    "code": norm_cid or code_val,
                    "chapter": cid_meta.get("chapter") if cid_meta else None,
                    "chapter_description": cid_meta.get("chapter_description") if cid_meta else None,
                    "diagnosis_rank": "primary",
                    "recorded_date": parsed_raw.get("recordedDate", datetime.utcnow().strftime("%Y-%m-%d")),
                    "_updated_at": datetime.utcnow().isoformat()
                })

        # 5. Procedures
        if "Procedure" in resource_groups:
            procs = resource_groups["Procedure"]
            for _, row in procs.iterrows():
                proc_id = str(row.get("id", ""))
                parsed_raw = _safe_parse_dict(row.get("raw_json", {}))

                subj_ref = parsed_raw.get("subject", {}).get("reference", "")
                pat_ref_id = subj_ref.replace("Patient/", "") if subj_ref else "unknown"

                enc_ref = parsed_raw.get("encounter", {}).get("reference", "")
                enc_ref_id = enc_ref.replace("Encounter/", "") if enc_ref else "unknown"

                codings = parsed_raw.get("code", {}).get("coding", [])
                sigtap_raw = codings[0].get("code", "0301010010") if codings else "0301010010"
                norm_sigtap, sigtap_meta = TerminologyService.normalize_sigtap(sigtap_raw)

                procedures_list.append({
                    "procedure_id": f"proc_fhir_{proc_id}",
                    "encounter_id": f"enc_fhir_{enc_ref_id}",
                    "patient_id": f"pat_fhir_{pat_ref_id}",
                    "code_system": "http://sigtap.datasus.gov.br",
                    "code": norm_sigtap or sigtap_raw,
                    "formatted_code": sigtap_meta.get("formatted_code") if sigtap_meta else sigtap_raw,
                    "group_description": sigtap_meta.get("group_description") if sigtap_meta else None,
                    "status": "completed",
                    "performed_date": parsed_raw.get("performedDateTime", datetime.utcnow().strftime("%Y-%m-%d")),
                    "_updated_at": datetime.utcnow().isoformat()
                })

        canonical = CanonicalDataset(
            dim_patients=pd.DataFrame(patients_list).drop_duplicates(subset=["patient_id"]) if patients_list else pd.DataFrame(),
            dim_organizations=pd.DataFrame(organizations_list).drop_duplicates(subset=["organization_id"]) if organizations_list else pd.DataFrame(),
            fct_encounters=pd.DataFrame(encounters_list).drop_duplicates(subset=["encounter_id"]) if encounters_list else pd.DataFrame(),
            fct_conditions=pd.DataFrame(conditions_list).drop_duplicates(subset=["condition_id"]) if conditions_list else pd.DataFrame(),
            fct_procedures=pd.DataFrame(procedures_list).drop_duplicates(subset=["procedure_id"]) if procedures_list else pd.DataFrame(),
            metadata={"source": "fhir_bronze", "row_count": len(df)}
        )
        logger.info(f"FHIR normalization summary: {canonical.summary()}")
        return canonical
