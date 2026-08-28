"""
Data Mart da Camada Gold: Ocupação e Capacidade de Leitos por Estado do Brasil (Mês de Julho).
Calcula a taxa de ocupação hospitalar (%) por Unidade da Federação e Região a partir do SIH e CNES.
"""
from typing import Optional
import pandas as pd
import numpy as np
from src.silver.ibge_nacional import resolver_uf_brasil
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_nacional_ocupacao_leitos(
    df_encounters: pd.DataFrame,
    df_cnes_leitos: Optional[pd.DataFrame] = None,
    dias_no_mes: int = 31
) -> pd.DataFrame:
    """
    Constrói o Data Mart dm_nacional_ocupacao_leitos.
    
    Fórmula Oficial:
    Taxa de Ocupação (%) = (Total de Dias de Permanência no Mês / (Total de Leitos CNES * 31 dias)) * 100
    """
    if df_encounters is None or df_encounters.empty:
        logger.warning("Base de internações vazia para cálculo de ocupação de leitos nacional.")
        return pd.DataFrame(columns=[
            "uf_sigla", "estado_nome", "regiao", "total_internacoes",
            "total_dias_permanencia", "leitos_totais_cnes", "leitos_uti_cnes",
            "capacidade_dias_leito_mes", "taxa_ocupacao_leitos_pct", "tempo_medio_permanencia_dias"
        ])

    logger.info("Calculando taxa de ocupação de leitos para todas as 27 UFs...")
    df = df_encounters.copy()

    # Normalização de UF
    if "uf" in df.columns:
        df["uf_clean"] = df["uf"].astype(str).str.upper().str.strip()
    elif "state" in df.columns:
        df["uf_clean"] = df["state"].astype(str).str.upper().str.strip()
    elif "municipality_code" in df.columns:
        df["uf_clean"] = df["municipality_code"].astype(str).str[:2].apply(lambda u: resolver_uf_brasil(u)[0])
    else:
        df["uf_clean"] = "CE"

    df["dias_perm"] = pd.to_numeric(df.get("length_of_stay_days", 0), errors="coerce").fillna(0)

    # Agrupamento por Estado
    grouped = df.groupby("uf_clean").agg(
        total_internacoes=("dias_perm", "count"),
        total_dias_permanencia=("dias_perm", "sum"),
    ).reset_index()

    grouped.rename(columns={"uf_clean": "uf_sigla"}, inplace=True)

    # Resolução de Nomes Oficiais de Estado e Região
    grouped["estado_nome"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[1])
    grouped["regiao"] = grouped["uf_sigla"].apply(lambda u: resolver_uf_brasil(u)[2])

    # Enriquecimento com Leitos do CNES (se disponível) ou estimativa referencial por UF
    # Censo CNES Brasil: ~480.000 leitos totais SUS e Privados distribuídos pelas 27 UFs
    leitos_ref_brasil = {
        "SP": 115000, "MG": 48000, "RJ": 42000, "BA": 32000, "RS": 31000, "PR": 29000,
        "CE": 22000, "PE": 21000, "SC": 18000, "GO": 17000, "MA": 15000, "PA": 14000,
        "PB": 11000, "ES": 10500, "MT": 9500, "RN": 8500, "PI": 8000, "AM": 7800,
        "AL": 7500, "MS": 7200, "DF": 7000, "SE": 5500, "RO": 4800, "TO": 3800,
        "AC": 2100, "AP": 1800, "RR": 1400
    }

    if df_cnes_leitos is not None and not df_cnes_leitos.empty:
        cnes_df = df_cnes_leitos.copy()
        if "uf" in cnes_df.columns and "leitos_totais" in cnes_df.columns:
            cnes_agg = cnes_df.groupby("uf").agg(
                leitos_totais_cnes=("leitos_totais", "sum"),
                leitos_uti_cnes=("leitos_uti", "sum")
            ).reset_index()
            grouped = grouped.merge(cnes_agg, left_on="uf_sigla", right_on="uf", how="left")
            grouped["leitos_totais_cnes"] = grouped["leitos_totais_cnes"].fillna(
                grouped["uf_sigla"].map(leitos_ref_brasil).fillna(1000)
            ).astype(int)
            grouped["leitos_uti_cnes"] = grouped["leitos_uti_cnes"].fillna(
                (grouped["leitos_totais_cnes"] * 0.12).astype(int)
            ).astype(int)
        else:
            grouped["leitos_totais_cnes"] = grouped["uf_sigla"].map(leitos_ref_brasil).fillna(2000).astype(int)
            grouped["leitos_uti_cnes"] = (grouped["leitos_totais_cnes"] * 0.12).astype(int)
    else:
        grouped["leitos_totais_cnes"] = grouped["uf_sigla"].map(leitos_ref_brasil).fillna(2000).astype(int)
        grouped["leitos_uti_cnes"] = (grouped["leitos_totais_cnes"] * 0.12).astype(int)

    # Cálculo da Capacidade Máxima e Taxa de Ocupação
    grouped["capacidade_dias_leito_mes"] = grouped["leitos_totais_cnes"] * dias_no_mes

    grouped["taxa_ocupacao_leitos_pct"] = np.where(
        grouped["capacidade_dias_leito_mes"] > 0,
        np.round((grouped["total_dias_permanencia"] / grouped["capacidade_dias_leito_mes"]) * 100.0, 2),
        0.0
    )

    grouped["tempo_medio_permanencia_dias"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round(grouped["total_dias_permanencia"] / grouped["total_internacoes"], 1),
        0.0
    )

    # Ordenar pelos estados com maior taxa de ocupação
    grouped = grouped.sort_values(by="taxa_ocupacao_leitos_pct", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart de Ocupação de Leitos gerado com sucesso para {len(grouped)} estados.")
    return grouped
