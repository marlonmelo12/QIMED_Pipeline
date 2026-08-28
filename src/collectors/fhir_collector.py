"""
Coletor de Dados Sintéticos em padrão FHIR R4 para o QIMED.
Gera pacientes, encontros, condições clínicas e procedimentos sintéticos realistas.
"""
import random
import uuid
import hashlib
from datetime import datetime, timedelta
import pandas as pd
from typing import Any, List, Dict

from src.collectors.base import BaseCollector, CollectorConfig
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Dados brasileiros realistas para geração sintética
_FIRST_NAMES = [
    "João", "Maria", "Pedro", "Ana", "Carlos", "Francisca", "Lucas",
    "Juliana", "Rafael", "Beatriz", "Matheus", "Fernanda", "Gabriel",
    "Camila", "Daniel", "Larissa", "Bruno", "Aline", "Gustavo", "Patrícia",
]
_LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira",
    "Almeida", "Nascimento", "Lima", "Araújo", "Melo", "Barbosa",
    "Ribeiro", "Martins", "Carvalho", "Gomes", "Pereira", "Costa",
]
_CID10_CODES = [
    "A00", "A09", "B15", "B24", "C34", "D50", "E10", "E11",
    "F32", "G40", "I10", "I21", "J06", "J18", "K35", "M54",
    "N39", "O80", "R10", "S72", "Z00",
]
_MUNICIPALITIES = [
    "230440", "230370", "231290", "230760", "231130",  # Ceará
    "355030", "354890", "354780",  # São Paulo
    "330455", "330170",  # Rio de Janeiro
]


def _fake_cpf() -> str:
    """Gera uma string simulada no formato de CPF para testes."""
    digits = [random.randint(0, 9) for _ in range(11)]
    return "".join(map(str, digits))


class FhirSyntheticCollector(BaseCollector):
    """
    Gera Bundles FHIR R4 sintéticos com dados demográficos e clínicos brasileiros realistas.
    Recursos: Patient, Encounter, Condition, Observation, Procedure, Organization.
    """

    def __init__(
        self,
        num_patients: int = 100,
        encounters_per_patient: int = 3,
        config: CollectorConfig = None,
    ):
        super().__init__(config)
        self.num_patients = num_patients
        self.encounters_per_patient = encounters_per_patient

    def get_source_type(self) -> str:
        return "fhir_synthetic"

    def fetch(self) -> List[Dict]:
        """Gera Bundles sintéticos em padrão FHIR R4."""
        logger.info(
            f"Gerando {self.num_patients} pacientes sintéticos "
            f"com {self.encounters_per_patient} encontros cada"
        )
        bundles: List[Dict] = []

        for _ in range(self.num_patients):
            patient_id = str(uuid.uuid4())
            given = random.choice(_FIRST_NAMES)
            family = random.choice(_LAST_NAMES)
            birth_year = random.randint(1940, 2010)
            birth_date = f"{birth_year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            gender = random.choice(["male", "female"])
            municipality = random.choice(_MUNICIPALITIES)

            patient = {
                "resourceType": "Patient",
                "id": patient_id,
                "name": [{"family": family, "given": [given]}],
                "birthDate": birth_date,
                "gender": gender,
                "identifier": [
                    {
                        "system": "http://rnds.saude.gov.br/fhir/r4/NamingSystem/cpf",
                        "value": _fake_cpf(),
                    }
                ],
                "address": [{"city": municipality, "country": "BR"}],
            }

            entries = [{"resource": patient}]

            # Organização hospitalar
            org_id = str(uuid.uuid4())
            org = {
                "resourceType": "Organization",
                "id": org_id,
                "name": f"Hospital {family}",
                "identifier": [
                    {"system": "http://cnes.saude.gov.br", "value": f"{random.randint(1000000, 9999999)}"}
                ],
            }
            entries.append({"resource": org})

            for enc_idx in range(self.encounters_per_patient):
                enc_id = str(uuid.uuid4())
                start = datetime(birth_year + random.randint(20, 60), random.randint(1, 12), random.randint(1, 28))
                end = start + timedelta(days=random.randint(1, 14))

                encounter = {
                    "resourceType": "Encounter",
                    "id": enc_id,
                    "status": random.choice(["finished", "in-progress"]),
                    "class": {"code": random.choice(["IMP", "AMB", "EMER"])},
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "serviceProvider": {"reference": f"Organization/{org_id}"},
                    "period": {
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    },
                }
                entries.append({"resource": encounter})

                # Condição / Diagnóstico
                condition = {
                    "resourceType": "Condition",
                    "id": str(uuid.uuid4()),
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "code": {
                        "coding": [
                            {"system": "http://hl7.org/fhir/sid/icd-10", "code": random.choice(_CID10_CODES)}
                        ]
                    },
                    "recordedDate": start.isoformat(),
                }
                entries.append({"resource": condition})

                # Observação clínica (sinais vitais)
                observation = {
                    "resourceType": "Observation",
                    "id": str(uuid.uuid4()),
                    "status": "final",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Frequência Cardíaca"}]},
                    "valueQuantity": {"value": random.randint(60, 120), "unit": "bpm"},
                    "effectiveDateTime": start.isoformat(),
                }
                entries.append({"resource": observation})

                # Procedimento médico
                procedure = {
                    "resourceType": "Procedure",
                    "id": str(uuid.uuid4()),
                    "status": "completed",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{enc_id}"},
                    "code": {
                        "coding": [{"system": "http://sigtap.datasus.gov.br", "code": f"0{random.randint(100000000, 999999999)}"}]
                    },
                    "performedDateTime": start.isoformat(),
                }
                entries.append({"resource": procedure})

            bundle = {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": entries,
            }
            bundles.append(bundle)

        logger.info(f"Bundles FHIR gerados: {len(bundles)}")
        return bundles

    def parse(self, raw_data: Any) -> pd.DataFrame:
        """Achata (flatten) Bundles FHIR em um DataFrame tabular (uma linha por recurso)."""
        bundles: List[Dict] = raw_data
        rows: List[Dict] = []

        for bundle in bundles:
            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                resource_type = resource.get("resourceType", "Unknown")
                resource_id = resource.get("id", "")

                row = {
                    "resourceType": resource_type,
                    "id": resource_id,
                    "raw_json": str(resource),
                }

                # Extrai campos específicos de Paciente para detecção de PII
                if resource_type == "Patient":
                    names = resource.get("name", [{}])
                    if names:
                        row["name_family"] = names[0].get("family", "")
                        row["name_given"] = ", ".join(names[0].get("given", []))
                    row["birthDate"] = resource.get("birthDate", "")
                    row["gender"] = resource.get("gender", "")
                    identifiers = resource.get("identifier", [])
                    for ident in identifiers:
                        if "cpf" in ident.get("system", "").lower():
                            row["cpf"] = ident.get("value", "")

                # Extrai campos de Encontro Hospitalar (Encounter)
                elif resource_type == "Encounter":
                    row["subject_ref"] = resource.get("subject", {}).get("reference", "")
                    period = resource.get("period", {})
                    row["period_start"] = period.get("start", "")
                    row["period_end"] = period.get("end", "")

                rows.append(row)

        df = pd.DataFrame(rows)
        logger.info(f"Recursos FHIR parseados: {len(df)} a partir de {len(bundles)} bundles")
        return df


# Alias para retrocompatibilidade
SyntheticFHIRCollector = FhirSyntheticCollector
