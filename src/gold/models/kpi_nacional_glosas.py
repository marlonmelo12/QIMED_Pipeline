"""
Data Mart da Camada Gold: Ranking Nacional de Glosas e Auditoria Financeira do SUS por Estado (Mês de Julho).
Consolida faturamento, aprovação e recusas financeiras em todas as 27 Unidades Federativas.
"""
from typing import Optional
import pandas as pd
import numpy as np
from src.silver.ibge_nacional import resolver_uf_brasil
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_nacional_ranking_glosas(df_sia_nacional: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói o Data Mart dm_nacional_ranking_glosas.
    Calcula valores faturados, aprovados e glosados por Estado e Região.
    """
    if df_sia_nacional is None or df_sia_nacional.empty:
        logger.warning("Base ambulatorial vazia para cálculo de glosas nacional.")
        return pd.DataFrame(columns=[
            "uf_sigla", "estado_nome", "regiao", "total_procedimentos_faturados",
            "total_procedimentos_aprovados", "total_procedimentos_glosados",
            "total_faturado_brl", "total_aprovado_brl", "total_glosado_brl",
            "taxa_glosa_pct"
        ])

    logger.info("Processando auditoria de glosas para todas as 27 UFs...")
    df = df_sia_nacional.copy()

    # Normalização de UF
    if "PA_UFMUN" in df.columns:
        df["uf_sigla"] = df["PA_UFMUN"].astype(str).str[:2].apply(lambda u: resolver_uf_brasil(u)[0])
    elif "uf" in df.columns:
        df["uf_sigla"] = df["uf"].astype(str).str.upper().str.strip()
    elif "municipality_code" in df.columns:
        df["uf_sigla"] = df["municipality_code"].astype(str).str[:2].apply(lambda u: resolver_uf_brasil(u)[0])
    else:
        df["uf_sigla"] = "CE"

    # Valores financeiros e quantitativos
    df["qtd_pro"] = pd.to_numeric(df.get("PA_QTDPRO", 1), errors="coerce").fillna(1)
    df["qtd_apr"] = pd.to_numeric(df.get("PA_QTDAPR", df["qtd_pro"]), errors="coerce").fillna(df["qtd_pro"])
    df["val_pro"] = pd.to_numeric(df.get("PA_VALPRO", 0.0), errors="coerce").fillna(0.0)
    df["val_apr"] = pd.to_numeric(df.get("PA_VALAPR", df["val_pro"]), errors="coerce").fillna(df["val_pro"])

    df["val_glo"] = np.maximum(0.0, df["val_pro"] - df["val_apr"])
    df["qtd_glo"] = np.maximum(0, df["qtd_pro"] - df["qtd_apr"])

    # Agrupamento Estadual
    grouped = df.groupby("uf_sigla").agg(
        total_procedimentos_faturados=("qtd_pro", "sum"),
        total_procedimentos_aprovados=("qtd_apr", "sum"),
        total_procedimentos_glosados=("qtd_glo", "sum"),
        total_faturado_brl=("val_pro", "sum"),
        total_aprovado_brl=("val_apr", "sum"),
        total_glosado_brl=("val_glo", "sum"),
    ).reset_index()

    # Nomes e Regiões
    grouped["estado_nome"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[1])
    grouped["regiao"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[2])

    # Taxa de Glosa (%)
    grouped["taxa_glosa_pct"] = np.where(
        grouped["total_faturado_brl"] > 0,
        np.round((grouped["total_glosado_brl"] / grouped["total_faturado_brl"]) * 100.0, 2),
        0.0
    )

    grouped["total_faturado_brl"] = np.round(grouped["total_faturado_brl"], 2)
    grouped["total_aprovado_brl"] = np.round(grouped["total_aprovado_brl"], 2)
    grouped["total_glosado_brl"] = np.round(grouped["total_glosado_brl"], 2)

    # Ordenar pelos estados com maior prejuízo financeiro por glosa
    grouped = grouped.sort_values(by="total_glosado_brl", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart Nacional de Glosas construído com sucesso para {len(grouped)} estados.")
    return grouped
