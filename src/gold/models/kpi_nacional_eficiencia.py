"""
Data Mart da Camada Gold: Eficiência Hospitalar, Custos e Mortalidade por Estado do Brasil (Mês de Julho).
Consolida volume de AIHs, custo médio por internação, tempo de permanência e desfechos clínicos por UF.
"""
from typing import Optional
import pandas as pd
import numpy as np
from src.silver.ibge_nacional import resolver_uf_brasil
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_nacional_eficiencia_mortalidade(df_encounters: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói o Data Mart dm_nacional_eficiencia_mortalidade.
    Calcula tempo de permanência, custos por AIH e mortalidade hospitalar por UF.
    """
    if df_encounters is None or df_encounters.empty:
        logger.warning("Base de internações vazia para cálculo de eficiência nacional.")
        return pd.DataFrame(columns=[
            "uf_sigla", "estado_nome", "regiao", "total_pacientes_unicos",
            "total_internacoes", "custo_total_brl", "custo_medio_aih_brl",
            "tempo_medio_permanencia_dias", "obitos_totais", "taxa_mortalidade_pct"
        ])

    logger.info("Calculando eficiência e mortalidade hospitalar nacional...")
    df = df_encounters.copy()

    # Normalização de UF
    if "uf" in df.columns:
        df["uf_sigla"] = df["uf"].astype(str).str.upper().str.strip()
    elif "state" in df.columns:
        df["uf_sigla"] = df["state"].astype(str).str.upper().str.strip()
    elif "municipality_code" in df.columns:
        df["uf_sigla"] = df["municipality_code"].astype(str).str[:2].apply(lambda u: resolver_uf_brasil(u)[0])
    else:
        df["uf_sigla"] = "CE"

    pat_col = "patient_master_id" if "patient_master_id" in df.columns else ("patient_id" if "patient_id" in df.columns else "encounter_id")
    df["dias_perm"] = pd.to_numeric(df.get("length_of_stay_days", 0), errors="coerce").fillna(0)
    df["custo"] = pd.to_numeric(df.get("total_cost_brl", 0.0), errors="coerce").fillna(0.0)
    
    if "discharge_disposition" in df.columns:
        df["is_obito"] = (df["discharge_disposition"] == "expired").astype(int)
    elif "MORTE" in df.columns:
        df["is_obito"] = df["MORTE"].astype(str).isin(["1", "true", "True"]).astype(int)
    else:
        df["is_obito"] = 0

    # Agrupamento Estadual
    grouped = df.groupby("uf_sigla").agg(
        total_pacientes_unicos=(pat_col, "nunique"),
        total_internacoes=("dias_perm", "count"),
        total_dias_permanencia=("dias_perm", "sum"),
        custo_total_brl=("custo", "sum"),
        obitos_totais=("is_obito", "sum"),
    ).reset_index()

    # Nomes Oficiais e Regiões
    grouped["estado_nome"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[1])
    grouped["regiao"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[2])

    # Métricas Derivadas
    grouped["custo_medio_aih_brl"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round(grouped["custo_total_brl"] / grouped["total_internacoes"], 2),
        0.0
    )

    grouped["tempo_medio_permanencia_dias"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round(grouped["total_dias_permanencia"] / grouped["total_internacoes"], 1),
        0.0
    )

    grouped["taxa_mortalidade_pct"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round((grouped["obitos_totais"] / grouped["total_internacoes"]) * 100.0, 2),
        0.0
    )

    grouped["custo_total_brl"] = np.round(grouped["custo_total_brl"], 2)
    grouped = grouped.sort_values(by="total_internacoes", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart de Eficiência Nacional construído com sucesso para {len(grouped)} estados.")
    return grouped
