"""
Data Mart: Atenção Primária e Internações Evitáveis - ICSAP (dm_icsap_prevention).
Identifica o percentual de internações por Condições Sensíveis à Atenção Primária (Portaria MS nº 221/2008)
e correlaciona com a cobertura de consultas e visitas da Estratégia Saúde da Família (SISAB).
"""
import pandas as pd
import numpy as np
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Grupos de CIDs da Lista Brasileira de Internações por Condições Sensíveis à Atenção Primária (ICSAP)
ICSAP_CID_PREFIXES = (
    # Doenças preveníveis por imunização (A33-A37, B16, etc.)
    "A33", "A34", "A35", "A36", "A37", "B16", "B26",
    # Gastroenterites infecciosas (A00-A09)
    "A00", "A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09",
    # Anemia (D50)
    "D50",
    # Diabetes mellitus (E10-E14)
    "E10", "E11", "E12", "E13", "E14",
    # Hipertensão arterial (I10, I11)
    "I10", "I11",
    # Asma e bronquites (J45, J46, J20, J21)
    "J45", "J46", "J20", "J21",
    # Infecção do trato urinário (N39)
    "N39",
    # Doenças inflamatórias pélvicas (N70-N76)
    "N70", "N71", "N72", "N73", "N74", "N75", "N76"
)


def build_dm_icsap_prevention(df_enc: pd.DataFrame, df_sisab: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Gera o Data Mart de Prevenção ICSAP cruzando internações hospitalares e dados da Atenção Primária (SISAB).
    """
    if df_enc is None or df_enc.empty:
        logger.warning("Base de internações vazia para cálculo de ICSAP.")
        return pd.DataFrame(columns=[
            "municipality_name", "total_internacoes", "internacoes_icsap_evitaveis",
            "taxa_icsap_pct", "custo_icsap_brl", "consultas_aps_realizadas", "visitas_domiciliares_acs"
        ])

    logger.info("Calculando taxa de Internações por Condições Sensíveis à Atenção Primária (ICSAP)...")
    df = df_enc.copy()

    # Determina coluna de agrupamento territorial ou hospitalar
    if "municipality_name" in df.columns:
        mun_name_col = "municipality_name"
    elif "municipality_code" in df.columns:
        mun_name_col = "municipality_code"
    elif "hospital_name" in df.columns:
        mun_name_col = "hospital_name"
    else:
        df["municipality_name"] = "Ceará (Estadual)"
        mun_name_col = "municipality_name"

    cid_col = "primary_diagnosis_code" if "primary_diagnosis_code" in df.columns else ("DIAG_PRINC" if "DIAG_PRINC" in df.columns else "cid")

    df["cid_clean"] = df.get(cid_col, "").astype(str).str.upper().str.replace(".", "", regex=False)
    df["is_icsap"] = df["cid_clean"].str.startswith(ICSAP_CID_PREFIXES, na=False).astype("int8")
    df["custo"] = pd.to_numeric(df.get("total_cost_brl", 0.0), errors="coerce").fillna(0.0)

    # Agregação municipal de internações
    grouped = df.groupby(mun_name_col).agg(
        total_internacoes=(cid_col if cid_col in df.columns else mun_name_col, "count"),
        internacoes_icsap_evitaveis=("is_icsap", "sum"),
        custo_icsap_brl=("custo", lambda c: np.round(c[df.loc[c.index, "is_icsap"] == 1].sum(), 2)),
    ).reset_index()

    from src.silver.terminology_names import resolver_nome_municipio

    grouped.rename(columns={mun_name_col: "municipality_name"}, inplace=True)
    grouped["municipality_name"] = grouped["municipality_name"].apply(resolver_nome_municipio)

    grouped["taxa_icsap_pct"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round((grouped["internacoes_icsap_evitaveis"] / grouped["total_internacoes"]) * 100.0, 2),
        0.0
    )

    # Cruzamento com dados da Atenção Básica (SISAB)
    if df_sisab is not None and not df_sisab.empty:
        sisab_ref = df_sisab.copy()
        sis_mun_col = "municipality_name" if "municipality_name" in sisab_ref.columns else "CO_MUNICIPIO_IBGE"
        sisab_ref["atend"] = pd.to_numeric(sisab_ref.get("QT_ATENDIMENTOS", 0), errors="coerce").fillna(0)
        sisab_ref["visitas"] = pd.to_numeric(sisab_ref.get("QT_VISITAS_DOMICILIARES", 0), errors="coerce").fillna(0)

        sisab_agg = sisab_ref.groupby(sis_mun_col).agg(
            consultas_aps_realizadas=("atend", "sum"),
            visitas_domiciliares_acs=("visitas", "sum")
        ).reset_index()
        sisab_agg.rename(columns={sis_mun_col: "municipality_name"}, inplace=True)

        grouped = grouped.merge(sisab_agg, on="municipality_name", how="left")
        grouped["consultas_aps_realizadas"] = grouped["consultas_aps_realizadas"].fillna(0).astype(int)
        grouped["visitas_domiciliares_acs"] = grouped["visitas_domiciliares_acs"].fillna(0).astype(int)
    else:
        grouped["consultas_aps_realizadas"] = 0
        grouped["visitas_domiciliares_acs"] = 0

    grouped = grouped.sort_values(by="taxa_icsap_pct", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart de Prevenção ICSAP construído para {len(grouped)} municípios.")
    return grouped
