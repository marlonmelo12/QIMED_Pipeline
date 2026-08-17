"""
Mapper Semantico para a ANS / D-TISS (Agencia Nacional de Saude Suplementar).
Transforma dados de operadoras e beneficiarios privados na dimensao dim_health_plans.
"""
from typing import Dict, Any, Optional
import pandas as pd

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.terminology_names import resolver_nome_municipio
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Estimativa populacional de referencia por municipio do Acre (Censo IBGE)
POPULACAO_IBGE_AC = {
    "120040": 419452, # Rio Branco
    "120020": 89072,  # Cruzeiro do Sul
    "120050": 46511,  # Sena Madureira
    "120060": 43464,  # Tarauacá
    "120030": 35426,  # Feijó
    "120010": 26702,  # Brasiléia
    "120045": 23236,  # Senador Guiomard
    "120038": 19955,  # Plácido de Castro
    "120070": 19733,  # Xapuri
    "120033": 19311,  # Mâncio Lima
    "120035": 17093,  # Marechal Thaumaturgo
    "120080": 18793,  # Porto Acre
    "120001": 15533,  # Acrelândia
    "120042": 19310,  # Rodrigues Alves
    "120013": 12973,  # Bujari
    "120034": 9581,   # Manoel Urbano
    "120025": 18733,  # Epitaciolândia
    "120032": 9222,   # Jordão
    "120043": 6732,   # Santa Rosa do Purus
    "120039": 12241,  # Porto Walter
    "120017": 10392,  # Capixaba
    "120005": 7534    # Assis Brasil
}

class AnsMapper(BaseSemanticMapper):
    """Mapeia operadoras, beneficiarios e cobertura privada da ANS para dim_health_plans."""

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df is None or df.empty:
            return CanonicalDataset()

        pdf = df.copy()

        # Calcular taxa de cobertura privada por municipio (%)
        taxas_cobertura = []
        for _, row in pdf.iterrows():
            m_code = str(row.get("CD_MUNICIPIO_IBGE", ""))
            ben = float(row.get("NR_BENEFICIARIOS_ATIVOS", 0))
            pop = POPULACAO_IBGE_AC.get(m_code, 30000)
            taxa = (ben / pop) * 100.0
            taxas_cobertura.append(round(taxa, 2))

        pdf["taxa_cobertura_privada_pct"] = taxas_cobertura

        # Dimensao dim_health_plans
        dim_health_plans = pd.DataFrame({
            "plan_id": "ans_plan_" + pdf["CD_OPERADORA"].astype(str) + "_" + pdf["CD_MUNICIPIO_IBGE"].astype(str),
            "ans_operator_code": pdf["CD_OPERADORA"].astype(str),
            "operator_name": pdf["RAZAO_SOCIAL"].astype(str),
            "modality": pdf["MODALIDADE_OPERADORA"].astype(str),
            "municipality_code": pdf["CD_MUNICIPIO_IBGE"].astype(str),
            "municipality_name": pdf["CD_MUNICIPIO_IBGE"].apply(resolver_nome_municipio),
            "state": pdf["SG_UF"].astype(str),
            "competence": pdf["COMPETENCIA"].astype(str),
            "active_beneficiaries": pdf["NR_BENEFICIARIOS_ATIVOS"].astype(int),
            "elderly_beneficiaries": pdf["NR_BENEFICIARIOS_IDOSOS"].astype(int),
            "total_private_expenditure_brl": pdf["DESPESA_ASSISTENCIAL_TOTAL"].astype(float),
            "loss_ratio_pct": pdf["SINISTRALIDADE_PCT"].astype(float),
            "private_coverage_ratio_pct": pdf["taxa_cobertura_privada_pct"]
        })

        logger.info(f"AnsMapper gerou {len(dim_health_plans)} registros para dim_health_plans.")
        return CanonicalDataset(
            dim_health_plans=dim_health_plans,
            metadata=source_metadata or {}
        )
