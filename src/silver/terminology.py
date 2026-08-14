"""
Brazilian Health Terminology Service for QIMED DataQore.
Normalizes and validates clinical and administrative coding systems:
- CID-10 (International Classification of Diseases 10th Revision)
- SIGTAP (Sistema de Gerenciamento da Tabela de Procedimentos do SUS)
- IBGE Municipality Codes
- CBO (Classificação Brasileira de Ocupações)
"""
import re
from typing import Dict, Any, Optional, Tuple
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Standard FHIR System URIs for Brazilian healthcare
FHIR_SYSTEM_CID10 = "http://hl7.org/fhir/sid/icd-10"
FHIR_SYSTEM_SIGTAP = "http://sigtap.datasus.gov.br"
FHIR_SYSTEM_IBGE = "http://ibge.gov.br/cidades"
FHIR_SYSTEM_CBO = "http://cbo.mte.gov.br"
FHIR_SYSTEM_CNES = "http://cnes.saude.gov.br"
FHIR_SYSTEM_CPF = "http://rnds.saude.gov.br/fhir/r4/NamingSystem/cpf"

# CID-10 Chapters mapping (A00-Z99)
CID10_CHAPTERS = {
    "I": ("A00", "B99", "Algumas doenças infecciosas e parasitárias"),
    "II": ("C00", "D48", "Neoplasias (tumores)"),
    "III": ("D50", "D89", "Doenças do sangue e dos órgãos hematopoéticos"),
    "IV": ("E00", "E90", "Doenças endócrinas, nutricionais e metabólicas"),
    "V": ("F00", "F99", "Transtornos mentais e comportamentais"),
    "VI": ("G00", "G99", "Doenças do sistema nervoso"),
    "VII": ("H00", "H59", "Doenças do olho e anexos"),
    "VIII": ("H60", "H95", "Doenças do ouvido e da apófise mastóide"),
    "IX": ("I00", "I99", "Doenças do aparelho circulatório"),
    "X": ("J00", "J99", "Doenças do aparelho respiratório"),
    "XI": ("K00", "K93", "Doenças do aparelho digestivo"),
    "XII": ("L00", "L99", "Doenças da pele e do tecido subcutâneo"),
    "XIII": ("M00", "M99", "Doenças do sistema osteomuscular e do tecido conjuntivo"),
    "XIV": ("N00", "N99", "Doenças do aparelho geniturinário"),
    "XV": ("O00", "O99", "Gravidez, parto e puerpério"),
    "XVI": ("P00", "P96", "Algumas afecções originadas no período perinatal"),
    "XVII": ("Q00", "Q99", "Malformações congênitas, deformidades e anomalias cromossômicas"),
    "XVIII": ("R00", "R99", "Sintomas, sinais e achados anormais de exames clínicos e de laboratório"),
    "XIX": ("S00", "T98", "Lesões, envenenamento e algumas outras conseqüências de causas externas"),
    "XX": ("V01", "Y98", "Causas externas de morbidade e de mortalidade"),
    "XXI": ("Z00", "Z99", "Fatores que influenciam o estado de saúde e o contato com os serviços de saúde"),
}

# SIGTAP Groups (first 2 digits)
SIGTAP_GROUPS = {
    "01": "Ações de promoção e prevenção em saúde",
    "02": "Procedimentos com finalidade diagnóstica",
    "03": "Procedimentos clínicos",
    "04": "Procedimentos cirúrgicos",
    "05": "Transplantes de órgãos, tecidos e células",
    "06": "Medicamentos",
    "07": "Órteses, próteses e materiais especiais (OPM)",
    "08": "Ações complementares da atenção à saúde",
}

# Brazilian State Codes (IBGE prefix)
UF_IBGE_PREFIX = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF"
}


class TerminologyService:
    """
    Standardization, lookup, and validation engine for Brazilian healthcare terminologies.
    """

    @staticmethod
    def normalize_cid10(code: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Normalize and validate a CID-10 diagnostic code.
        Returns (normalized_code, metadata_dict).
        Example: 'a090' -> ('A09.0', {'chapter': 'I', 'description': '...', 'valid': True})
        """
        if not code or str(code).strip() == "" or str(code).upper() in ("NAN", "NONE", "NULL"):
            return None, None

        raw = str(code).strip().upper()
        # Clean non-alphanumeric except dot
        clean = re.sub(r"[^A-Z0-9]", "", raw)
        if len(clean) < 3:
            return raw, {"valid": False, "reason": "Code too short"}

        # Extract letter and number
        category = clean[:3]
        subcat = clean[3:] if len(clean) > 3 else None

        if not re.match(r"^[A-Z][0-9]{2}$", category):
            return raw, {"valid": False, "reason": "Invalid CID-10 format"}

        formatted_code = f"{category}.{subcat}" if subcat else category

        # Identify chapter
        chapter_info = None
        for chapter, (start, end, desc) in CID10_CHAPTERS.items():
            if start <= category <= end:
                chapter_info = {
                    "chapter_number": chapter,
                    "chapter_description": desc
                }
                break

        return formatted_code, {
            "valid": True,
            "category": category,
            "subcategory": subcat,
            "formatted_code": formatted_code,
            "chapter": chapter_info.get("chapter_number") if chapter_info else "UNKNOWN",
            "chapter_description": chapter_info.get("chapter_description") if chapter_info else "Não identificado",
            "fhir_coding": {
                "system": FHIR_SYSTEM_CID10,
                "code": formatted_code,
                "display": chapter_info.get("chapter_description") if chapter_info else formatted_code
            }
        }

    @staticmethod
    def normalize_sigtap(code: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Normalize and validate a SIGTAP SUS procedure code (10 digits).
        Format: GG.SS.FF.NNN-D (Group.Subgroup.Form.Number-DV)
        """
        if not code or str(code).strip() == "" or str(code).upper() in ("NAN", "NONE", "NULL"):
            return None, None

        raw = str(code).strip()
        digits = re.sub(r"\D", "", raw)

        # Pad left with zero if 9 digits (common in DATASUS missing leading zero)
        if len(digits) == 9:
            digits = "0" + digits

        if len(digits) != 10:
            return raw, {"valid": False, "reason": f"Expected 10 digits, got {len(digits)}"}

        group_code = digits[:2]
        subgroup_code = digits[2:4]
        form_code = digits[4:6]
        item_code = digits[6:9]
        check_digit = digits[9]

        group_name = SIGTAP_GROUPS.get(group_code, "Outros procedimentos")
        formatted = f"{group_code}.{subgroup_code}.{form_code}.{item_code}-{check_digit}"

        return digits, {
            "valid": True,
            "raw_code": digits,
            "formatted_code": formatted,
            "group_code": group_code,
            "group_description": group_name,
            "fhir_coding": {
                "system": FHIR_SYSTEM_SIGTAP,
                "code": digits,
                "display": group_name
            }
        }

    @staticmethod
    def normalize_ibge_municipality(code: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Normalize IBGE 6-digit or 7-digit municipality code.
        Extracts UF code and UF abbreviation.
        """
        if not code or str(code).strip() == "" or str(code).upper() in ("NAN", "NONE", "NULL"):
            return None, None

        raw = str(code).strip()
        digits = re.sub(r"\D", "", raw)

        if len(digits) not in (6, 7):
            return raw, {"valid": False, "reason": "IBGE code must have 6 or 7 digits"}

        uf_prefix = digits[:2]
        uf_abbr = UF_IBGE_PREFIX.get(uf_prefix, "BR")

        return digits[:6], {
            "valid": True,
            "code_6digits": digits[:6],
            "code_7digits": digits if len(digits) == 7 else None,
            "uf_code": uf_prefix,
            "uf_abbreviation": uf_abbr,
            "fhir_coding": {
                "system": FHIR_SYSTEM_IBGE,
                "code": digits[:6],
                "display": f"Município UF {uf_abbr}"
            }
        }

    @staticmethod
    def build_fhir_concept(system: str, code: str, display: str = None) -> Dict[str, Any]:
        """
        Build standard FHIR R4 CodeableConcept / Coding structure.
        """
        return {
            "coding": [
                {
                    "system": system,
                    "code": str(code),
                    "display": display or str(code)
                }
            ],
            "text": display or str(code)
        }
