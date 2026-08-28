"""
Mapper Semantico para a ANS / D-TISS (Agencia Nacional de Saude Suplementar).
Transforma dados de operadoras e beneficiarios privados na dimensao dim_health_plans sem fabricacao de dados.
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
        pdf.columns = [c.lower() for c in pdf.columns]

        # Normalização de nomes de colunas conhecidas sem fabricar valores
        cd_op = pdf["cd_operadora"] if "cd_operadora" in pdf.columns else pdf.get("registro_ans", pd.Series([None] * len(pdf)))
        cd_mun = pdf["cd_municipio_ibge"] if "cd_municipio_ibge" in pdf.columns else pdf.get("cd_municipio", pd.Series([None] * len(pdf)))
        uf_col = pdf["uf"] if "uf" in pdf.columns else pdf.get("sg_uf", pd.Series([None] * len(pdf)))
        razao_col = pdf.get("razao_social", pd.Series([None] * len(pdf)))
        mod_col = pdf["modalidade"] if "modalidade" in pdf.columns else pdf.get("modalidade_operadora", pd.Series([None] * len(pdf)))
        comp_col = pdf.get("competencia", pd.Series([None] * len(pdf)))

        # Geração de plan_id determinístico baseado nos atributos presentes
        plan_ids = []
        for op, mun in zip(cd_op, cd_mun):
            if pd.notna(op) and pd.notna(mun):
                plan_ids.append(f"ans_plan_{op}_{mun}")
            elif pd.notna(op):
                plan_ids.append(f"ans_plan_{op}")
            else:
                plan_ids.append(None)

        # Calcular taxa de cobertura privada por municipio (%)
        taxas_cobertura = []
        for _, row in pdf.iterrows():
            m_code = str(row.get("cd_municipio_ibge", "") or "").strip()
            ben = row.get("nr_beneficiarios_ativos")
            if pd.notna(ben) and m_code in POPULACAO_IBGE_AC:
                pop = POPULACAO_IBGE_AC[m_code]
                taxa = (float(ben) / pop) * 100.0
                taxas_cobertura.append(round(taxa, 2))
            elif pd.notna(ben) and len(m_code) >= 6:
                taxa = (float(ben) / 30000.0) * 100.0
                taxas_cobertura.append(round(taxa, 2))
            else:
                taxas_cobertura.append(None)

        # Dimensao dim_health_plans com tipos anuláveis legítimos
        dim_health_plans = pd.DataFrame({
            "plan_id": plan_ids,
            "ans_operator_code": cd_op.astype("string"),
            "operator_name": razao_col.astype("string"),
            "modality": mod_col.astype("string"),
            "municipality_code": cd_mun.astype("string"),
            "municipality_name": cd_mun.apply(lambda m: resolver_nome_municipio(str(m)) if pd.notna(m) and str(m).strip() else None),
            "state": uf_col.astype("string"),
            "competence": comp_col.astype("string"),
            "active_beneficiaries": pd.to_numeric(pdf.get("nr_beneficiarios_ativos"), errors="coerce").astype("Int64"),
            "elderly_beneficiaries": pd.to_numeric(pdf.get("nr_beneficiarios_idosos"), errors="coerce").astype("Int64"),
            "total_private_expenditure_brl": pd.to_numeric(pdf.get("despesa_assistencial_total"), errors="coerce").astype("Float64"),
            "loss_ratio_pct": pd.to_numeric(pdf.get("sinistralidade_pct"), errors="coerce").astype("Float64"),
            "private_coverage_ratio_pct": pd.Series(taxas_cobertura, dtype="Float64")
        })

        logger.info(f"AnsMapper gerou {len(dim_health_plans)} registros para dim_health_plans.")
        return CanonicalDataset(
            dim_health_plans=dim_health_plans,
            metadata=source_metadata or {}
        )
