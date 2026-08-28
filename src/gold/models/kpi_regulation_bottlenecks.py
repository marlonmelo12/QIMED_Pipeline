"""
Data Mart: Gargalos de Regulação e Filas do SUS (dm_regulation_bottlenecks).
Calcula tempo médio de espera por leito de UTI/cirurgia, taxa de autorização e déficit de vagas regionais.
"""
import pandas as pd
import numpy as np

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_regulation_bottlenecks(df_ref: pd.DataFrame) -> pd.DataFrame:
    """
    Gera o Data Mart de Gargalos de Regulação a partir de fct_referrals (SISREG / CROSS).
    """
    if df_ref is None or df_ref.empty:
        logger.warning("Base de regulação vazia para cálculo de gargalos.")
        return pd.DataFrame(columns=[
            "municipality_name", "referral_type", "total_solicitacoes",
            "solicitacoes_autorizadas", "solicitacoes_pendentes_fila", "taxa_autorizacao_pct",
            "tempo_medio_espera_dias", "tempo_maximo_espera_dias"
        ])

    logger.info("Processando filas e tempos de espera da regulação...")
    df = df_ref.copy()

    mun_name_col = "municipality_name" if "municipality_name" in df.columns else "municipality_code"
    ref_type_col = "referral_type" if "referral_type" in df.columns else "TIPO_VAGA"
    status_col = "status" if "status" in df.columns else "STATUS_REGULACAO"
    wait_time_col = "wait_time_days" if "wait_time_days" in df.columns else "tempo_espera"

    df["wait_days"] = pd.to_numeric(df.get(wait_time_col, 0.0), errors="coerce").fillna(0.0)
    df["is_autorizada"] = df[status_col].astype(str).str.upper().str.contains("AUTORIZ").astype(int)
    df["is_pendente"] = df[status_col].astype(str).str.upper().str.contains("FILA|PENDENTE|AGUARD").astype(int)

    grouped = df.groupby([mun_name_col, ref_type_col]).agg(
        total_solicitacoes=(status_col, "count"),
        solicitacoes_autorizadas=("is_autorizada", "sum"),
        solicitacoes_pendentes_fila=("is_pendente", "sum"),
        tempo_medio_espera_dias=("wait_days", lambda w: np.round(w.mean(), 2)),
        tempo_maximo_espera_dias=("wait_days", lambda w: np.round(w.max(), 2)),
    ).reset_index()

    from src.silver.terminology_names import resolver_nome_municipio

    grouped.rename(columns={
        mun_name_col: "municipality_name",
        ref_type_col: "referral_type"
    }, inplace=True)
    grouped["municipality_name"] = grouped["municipality_name"].apply(resolver_nome_municipio)

    grouped["taxa_autorizacao_pct"] = np.where(
        grouped["total_solicitacoes"] > 0,
        np.round((grouped["solicitacoes_autorizadas"] / grouped["total_solicitacoes"]) * 100.0, 2),
        0.0
    )

    grouped = grouped.sort_values(by="tempo_medio_espera_dias", ascending=False).reset_index(drop=True)
    logger.info(f"Data Mart de Gargalos de Regulação construído com {len(grouped)} filas monitoradas.")
    return grouped
