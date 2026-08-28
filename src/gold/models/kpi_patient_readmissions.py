"""
Data Mart: Readmissões Precoces e Continuidade do Cuidado (dm_patient_readmissions).
Calcula a taxa de readmissão hospitalar em 30 dias (30-day Readmissions) por patologia e desfecho da linha de cuidado.
"""
import pandas as pd
import numpy as np
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_patient_readmissions(df_enc: pd.DataFrame, df_cond: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Gera o Data Mart de Readmissões Precoces em 30 dias rastreando o paciente via MPI.
    """
    if df_enc is None or df_enc.empty:
        logger.warning("Base de internações vazia para cálculo de readmissões.")
        return pd.DataFrame(columns=[
            "cid10_code", "disease_name", "chapter_description", "total_pacientes",
            "total_internacoes", "readmissoes_30_dias", "taxa_readmissao_30_dias_pct",
            "intervalo_medio_readmissao_dias", "custo_total_readmissoes_brl"
        ])

    logger.info("Calculando taxa longitudinal de readmissões em 30 dias...")
    df = df_enc.copy()

    # Identificadores essenciais
    pat_col = "patient_master_id" if "patient_master_id" in df.columns else ("patient_id" if "patient_id" in df.columns else None)
    if not pat_col:
        logger.warning("Coluna de paciente não encontrada em fct_encounters.")
        return pd.DataFrame()

    # Tratamento temporal
    df["dt_start"] = pd.to_datetime(df["period_start"], errors="coerce")
    df["dt_end"] = pd.to_datetime(df["period_end"], errors="coerce")
    df = df.dropna(subset=["dt_start"]).sort_values(by=[pat_col, "dt_start"]).reset_index(drop=True)

    # Identificação de internações subsequentes pelo MPI
    df["prev_pat"] = df[pat_col].shift(1)
    df["prev_end"] = df["dt_end"].shift(1)

    # Diferença em dias desde a última alta
    is_same_patient = (df[pat_col] == df["prev_pat"])
    df["dias_desde_alta"] = np.where(
        is_same_patient,
        (df["dt_start"] - df["prev_end"]).dt.total_seconds() / 86400.0,
        np.nan
    )

    # Readmissão em 30 dias (intervalo entre 0 e 30 dias após alta anterior)
    df["is_readmission_30d"] = (is_same_patient & (df["dias_desde_alta"] >= 0) & (df["dias_desde_alta"] <= 30)).astype(int)

    # Diagnósticos / Patologia
    cid_col = "primary_diagnosis_code" if "primary_diagnosis_code" in df.columns else "DIAG_PRINC"
    cid_name_col = "primary_diagnosis_name" if "primary_diagnosis_name" in df.columns else cid_col
    chapter_col = "primary_diagnosis_chapter" if "primary_diagnosis_chapter" in df.columns else "chapter"

    df["cid_clean"] = df.get(cid_col, "OUTROS").astype(str).str[:3]
    df["dis_name"] = df.get(cid_name_col, "Doença Geral")
    df["chap_desc"] = df.get(chapter_col, "Capítulo Geral")
    df["custo"] = pd.to_numeric(df.get("total_cost_brl", 0.0), errors="coerce").fillna(0.0)

    from src.silver.terminology_names import resolver_nome_doenca

    # Agregação por Categoria CID-10
    grouped = df.groupby(["cid_clean", "chap_desc"]).agg(
        total_pacientes=(pat_col, "nunique"),
        total_internacoes=(pat_col, "count"),
        readmissoes_30_dias=("is_readmission_30d", "sum"),
        intervalo_medio_readmissao_dias=("dias_desde_alta", lambda x: np.round(x[x.between(0, 30)].mean(), 1) if x.between(0, 30).any() else 0.0),
        custo_total_readmissoes_brl=("custo", lambda c: np.round(c[df.loc[c.index, "is_readmission_30d"] == 1].sum(), 2))
    ).reset_index()

    grouped.rename(columns={
        "cid_clean": "cid10_code",
        "chap_desc": "chapter_description"
    }, inplace=True)

    grouped["disease_name"] = grouped.apply(lambda r: resolver_nome_doenca(r["cid10_code"], r.get("chapter_description", "")), axis=1)

    grouped["taxa_readmissao_30_dias_pct"] = np.where(
        grouped["total_internacoes"] > 0,
        np.round((grouped["readmissoes_30_dias"] / grouped["total_internacoes"]) * 100.0, 2),
        0.0
    )

    # Ordenar pelas patologias com maiores readmissões
    grouped = grouped.sort_values(by="readmissoes_30_dias", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart de Readmissões construído com {len(grouped)} patologias monitoradas.")
    return grouped
