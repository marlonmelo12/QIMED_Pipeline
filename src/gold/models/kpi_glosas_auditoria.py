"""
Data Mart: Auditoria e Prevenção de Glosas Hospitalares / Ambulatoriais (dm_glosas_auditoria).
Calcula valores faturados, aprovados e glosados (prejuízos), além da taxa percentual de glosa.
"""
import pandas as pd
import numpy as np
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def build_dm_glosas_auditoria(df_sia: pd.DataFrame, df_cnes: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Gera o Data Mart de Auditoria de Glosas a partir da produção ambulatorial e hospitalar (SIA).
    """
    if df_sia is None or df_sia.empty:
        logger.warning("Base do SIA vazia para construção do Data Mart de Glosas.")
        return pd.DataFrame(columns=[
            "cnes_code", "hospital_name", "municipality_code", "municipality_name",
            "total_procedimentos_faturados", "total_procedimentos_aprovados", "total_procedimentos_glosados",
            "total_faturado_brl", "total_aprovado_brl", "total_glosado_brl", "taxa_glosa_pct"
        ])

    logger.info(f"Processando {len(df_sia)} registros para o Data Mart de Glosas...")
    df = df_sia.copy()

    # Normaliza nomes de colunas
    cnes_col = "PA_CODUNI" if "PA_CODUNI" in df.columns else "CNES"
    hosp_name_col = "hospital_name" if "hospital_name" in df.columns else cnes_col
    mun_col = "PA_UFMUN" if "PA_UFMUN" in df.columns else ("CODUFMUN" if "CODUFMUN" in df.columns else "municipality_code")
    mun_name_col = "municipality_name" if "municipality_name" in df.columns else mun_col

    val_pro_col = "PA_VALPRO" if "PA_VALPRO" in df.columns else "valor_faturado"
    val_apr_col = "PA_VALAPR" if "PA_VALAPR" in df.columns else "valor_aprovado"
    qtd_pro_col = "PA_QTDPRO" if "PA_QTDPRO" in df.columns else "qtd_faturada"
    qtd_apr_col = "PA_QTDAPR" if "PA_QTDAPR" in df.columns else "qtd_aprovada"

    # Conversão numérica segura
    df["val_pro"] = pd.to_numeric(df[val_pro_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
    df["val_apr"] = pd.to_numeric(df[val_apr_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
    df["qtd_pro"] = pd.to_numeric(df[qtd_pro_col], errors="coerce").fillna(1)
    df["qtd_apr"] = pd.to_numeric(df[qtd_apr_col], errors="coerce").fillna(0)

    # Cálculo da glosa financeira e de volume
    df["val_glo"] = np.maximum(0.0, df["val_pro"] - df["val_apr"])
    df["qtd_glo"] = np.maximum(0, df["qtd_pro"] - df["qtd_apr"])

    # Agrupamento por Prestador e Município
    groupby_cols = list(dict.fromkeys([cnes_col, hosp_name_col, mun_col, mun_name_col]))
    grouped = df.groupby(groupby_cols).agg(
        total_procedimentos_faturados=("qtd_pro", "sum"),
        total_procedimentos_aprovados=("qtd_apr", "sum"),
        total_procedimentos_glosados=("qtd_glo", "sum"),
        total_faturado_brl=("val_pro", "sum"),
        total_aprovado_brl=("val_apr", "sum"),
        total_glosado_brl=("val_glo", "sum"),
    ).reset_index()

    from src.silver.terminology_names import resolver_nome_hospital, resolver_nome_municipio

    rename_map = {}
    if cnes_col in grouped.columns: rename_map[cnes_col] = "cnes_code"
    if mun_col in grouped.columns: rename_map[mun_col] = "municipality_code"
    grouped.rename(columns=rename_map, inplace=True)

    grouped["hospital_name"] = grouped["cnes_code"].apply(resolver_nome_hospital)
    grouped["municipality_name"] = grouped["municipality_code"].apply(resolver_nome_municipio)

    # Taxa percentual de glosa
    grouped["taxa_glosa_pct"] = np.where(
        grouped["total_faturado_brl"] > 0,
        np.round((grouped["total_glosado_brl"] / grouped["total_faturado_brl"]) * 100.0, 2),
        0.0
    )

    grouped["total_faturado_brl"] = np.round(grouped["total_faturado_brl"], 2)
    grouped["total_aprovado_brl"] = np.round(grouped["total_aprovado_brl"], 2)
    logger.info(f"Data Mart de Glosas construído com sucesso: {len(grouped)} prestadores agregados.")
    return grouped


def build_dm_motivos_glosas_hospitalares(df_glosas: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói o ranking e agregação dos motivos reais de rejeição/glosa hospitalar do SUS (SIH-ER/RJ).
    """
    if df_glosas is None or df_glosas.empty:
        logger.warning("Base de glosas hospitalares vazia.")
        return pd.DataFrame(columns=[
            "codigo_motivo_glosa", "descricao_motivo_glosa", "total_aih_glosadas",
            "valor_total_glosado_brl", "percentual_valor_glosado_pct"
        ])
    
    df = df_glosas.copy()
    val_col = "valor_glosado_brl" if "valor_glosado_brl" in df.columns else ("VAL_TOT" if "VAL_TOT" in df.columns else "val_glo")
    df["val_glo"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0)
    
    cod_col = "codigo_motivo_glosa" if "codigo_motivo_glosa" in df.columns else ("CO_ERRO" if "CO_ERRO" in df.columns else "codigo_motivo")
    desc_col = "descricao_motivo_glosa" if "descricao_motivo_glosa" in df.columns else ("DS_ERRO" if "DS_ERRO" in df.columns else "descricao_motivo")
    
    if cod_col not in df.columns:
        df[cod_col] = "NAO_INFORMADO"
    if desc_col not in df.columns:
        df[desc_col] = "Motivo de Rejeição Não Especificado"

    aih_col = "numero_aih" if "numero_aih" in df.columns else ("N_AIH" if "N_AIH" in df.columns else cod_col)

    grouped = df.groupby([cod_col, desc_col]).agg(
        total_aih_glosadas=(aih_col, "count"),
        valor_total_glosado_brl=("val_glo", "sum")
    ).reset_index()
    
    total_geral = grouped["valor_total_glosado_brl"].sum()
    grouped["percentual_valor_glosado_pct"] = np.where(
        total_geral > 0,
        np.round((grouped["valor_total_glosado_brl"] / total_geral) * 100.0, 2),
        0.0
    )
    grouped.rename(columns={cod_col: "codigo_motivo_glosa", desc_col: "descricao_motivo_glosa"}, inplace=True)
    grouped.sort_values(by="valor_total_glosado_brl", ascending=False, inplace=True)
    return grouped

