"""
Data Mart: Eficiência Hospitalar, Capacidade e Mortalidade (dm_hospital_efficiency).
Calcula tempo médio de permanência (LOS), custo médio de internação, taxa de mortalidade e giro de leitos.
"""
import pandas as pd
import numpy as np
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_hospital_efficiency(df_enc: pd.DataFrame, df_cnes: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Gera o Data Mart de Eficiência Hospitalar cruzando fct_encounters e cnes_estabelecimentos.
    """
    if df_enc is None or df_enc.empty:
        logger.warning("Base de internações vazia para construção do Data Mart de Eficiência Hospitalar.")
        return pd.DataFrame(columns=[
            "cnes_code", "hospital_name", "municipality_name", "total_internacoes",
            "total_dias_permanencia", "tempo_medio_permanencia_dias", "custo_total_brl",
            "custo_medio_internacao_brl", "obitos_hospitalares", "taxa_mortalidade_pct",
            "leitos_totais_cadastrados", "leitos_uti_cadastrados"
        ])

    logger.info(f"Processando {len(df_enc)} internações para o Data Mart de Eficiência Hospitalar...")
    df = df_enc.copy()

    # Normalização de CNES
    if "organization_id" in df.columns:
        df["cnes_code"] = df["organization_id"].astype(str).str.replace("org_cnes_", "").str.zfill(7)
    else:
        df["cnes_code"] = df.get("CNES", "UNKNOWN").astype(str).str.zfill(7)

    hosp_name_col = "hospital_name" if "hospital_name" in df.columns else "cnes_code"
    
    # Tratamento numérico
    df["dias_perm"] = pd.to_numeric(df.get("length_of_stay_days", 0), errors="coerce").fillna(0)
    df["custo_tot"] = pd.to_numeric(df.get("total_cost_brl", 0.0), errors="coerce").fillna(0.0)
    
    # Detecção de óbito
    if "discharge_disposition" in df.columns:
        df["is_obito"] = (df["discharge_disposition"] == "expired").astype(int)
    elif "MORTE" in df.columns:
        df["is_obito"] = df["MORTE"].astype(str).isin(["1", "true", "True"]).astype(int)
    else:
        df["is_obito"] = 0

    from src.silver.terminology_names import resolver_nome_hospital, resolver_nome_municipio
    
    # Agrupamento hospitalar
    grp_cols = list(dict.fromkeys(["cnes_code", hosp_name_col]))
    grouped = df.groupby(grp_cols).agg(
        total_internacoes=("encounter_id" if "encounter_id" in df.columns else "cnes_code", "count"),
        total_dias_permanencia=("dias_perm", "sum"),
        custo_total_brl=("custo_tot", "sum"),
        obitos_hospitalares=("is_obito", "sum"),
    ).reset_index()

    # Aplicação de nomes oficiais
    grouped["hospital_name"] = grouped["cnes_code"].apply(resolver_nome_hospital)

    # Métricas analíticas derivadas
    grouped["tempo_medio_permanencia_dias"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round(grouped["total_dias_permanencia"] / grouped["total_internacoes"], 1),
        0.0
    )

    grouped["custo_medio_internacao_brl"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round(grouped["custo_total_brl"] / grouped["total_internacoes"], 2),
        0.0
    )

    grouped["taxa_mortalidade_pct"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round((grouped["obitos_hospitalares"] / grouped["total_internacoes"]) * 100.0, 2),
        0.0
    )

    # Enriquecimento com Leitos do CNES se fornecido
    if df_cnes is not None and not df_cnes.empty:
        cnes_ref = df_cnes.copy()
        cnes_ref["cnes_clean"] = cnes_ref["CNES"].astype(str).str.zfill(7)
        cnes_ref["leitos_tot"] = pd.to_numeric(cnes_ref.get("LEITOS", 0), errors="coerce").fillna(0)
        cnes_ref["leitos_uti_val"] = pd.to_numeric(cnes_ref.get("LEITOS_UTI", 0), errors="coerce").fillna(0)
        cnes_ref["mun_name"] = cnes_ref.get("municipality_name", "Município")

        cnes_agg = cnes_ref.groupby("cnes_clean").agg(
            municipality_name=("mun_name", "first"),
            leitos_totais_cadastrados=("leitos_tot", "max"),
            leitos_uti_cadastrados=("leitos_uti_val", "max")
        ).reset_index()

        grouped = grouped.merge(cnes_agg, left_on="cnes_code", right_on="cnes_clean", how="left")
        grouped.drop(columns=["cnes_clean"], inplace=True)
        grouped["municipality_name"] = grouped["municipality_name"].fillna("Não Mapeado")
        grouped["leitos_totais_cadastrados"] = grouped["leitos_totais_cadastrados"].fillna(0).astype(int)
        grouped["leitos_uti_cadastrados"] = grouped["leitos_uti_cadastrados"].fillna(0).astype(int)
    else:
        grouped["municipality_name"] = "Ceará (Estadual)"
        grouped["leitos_totais_cadastrados"] = 0
        grouped["leitos_uti_cadastrados"] = 0

    logger.info(f"Data Mart de Eficiência Hospitalar gerado com {len(grouped)} hospitais.")
    return grouped
