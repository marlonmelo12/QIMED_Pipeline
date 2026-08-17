"""
Mineracao Profunda de Achados Clinicos, Operacionais e Epidemiologicos no Lakehouse (Acre - Jan/2025).
"""
import os
import sys
import pandas as pd
import numpy as np
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.silver.terminology_names import (
    resolver_nome_doenca,
    resolver_nome_procedimento,
    resolver_nome_municipio,
    resolver_nome_hospital
)

silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
bronze_base = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "datasus")

enc_df = DeltaTable(os.path.join(silver_base, "fct_encounters")).to_pandas()
pat_df = DeltaTable(os.path.join(silver_base, "dim_patients")).to_pandas()
cond_df = DeltaTable(os.path.join(silver_base, "fct_conditions")).to_pandas()
proc_df = DeltaTable(os.path.join(silver_base, "fct_procedures")).to_pandas()
cnes_df = DeltaTable(os.path.join(bronze_base, "cnes")).to_pandas()
sia_df = DeltaTable(os.path.join(bronze_base, "sia")).to_pandas()

# Filtrar para Jan/2025
df = enc_df[enc_df["_source_file"].str.contains("2501", na=False)].copy()
df["hospital_name"] = df["organization_id"].apply(resolver_nome_hospital)
df["disease_name"] = df.apply(lambda r: resolver_nome_doenca(r["primary_diagnosis_code"], r.get("primary_diagnosis_chapter", "")), axis=1)

print("=" * 85)
print("  MINERACAO AVANCADA DE DADOS E ACHADOS CLINICO-OPERACIONAIS")
print("=" * 85)

# --- ACHADO 1: Evasao Hospitalar e Transferencias Nao Concluidas ---
print("\n[1. ANALISE DE DESFECHOS ESPECIAIS: EVASOES E TRANSFERENCIAS]")
print(df["discharge_disposition"].value_counts())

# --- ACHADO 2: Migracao de Pacientes entre Municipios (Evasao Regional de Saude) ---
df_merged_pat = df.merge(pat_df, on="patient_master_id", how="left")
df_merged_pat["mun_paciente"] = df_merged_pat["municipality_code"].apply(resolver_nome_municipio)

# Relacionar municipio do paciente com o hospital onde internou
cnes_map = cnes_df[["CNES", "CODUFMUN"]].drop_duplicates("CNES")
cnes_map["organization_id"] = "org_cnes_" + cnes_map["CNES"].astype(str)
cnes_map["mun_hospital"] = cnes_map["CODUFMUN"].apply(resolver_nome_municipio)

df_migracao = df_merged_pat.merge(cnes_map[["organization_id", "mun_hospital"]], on="organization_id", how="left")
df_migracao["viajou_para_internar"] = df_migracao["mun_paciente"] != df_migracao["mun_hospital"]

taxa_migracao = df_migracao["viajou_para_internar"].mean() * 100
print(f"\n[2. MIGRACAO INTERMUNICIPAL DE PACIENTES]")
print(f"Total de pacientes que precisaram sair de sua cidade para internar: {df_migracao['viajou_para_internar'].sum():,} ({taxa_migracao:.2f}%)")

# Top rotas de migracao
rotas = df_migracao[df_migracao["viajou_para_internar"]].groupby(["mun_paciente", "hospital_name"]).size().reset_index(name="total_pacientes").sort_values("total_pacientes", ascending=False).head(8)
print("\nTop Rotas de Deslocamento Intermunicipal para Internacao:")
print(rotas.to_string(index=False))

# --- ACHADO 3: Cirurgias de Alta Complexidade vs. Cirurgias de Urgencia (Cesarianas) ---
partos_normais = len(df[df["primary_diagnosis_code"] == "O80.0"])
cesarianas = len(df[df["primary_diagnosis_code"].str.startswith("O82") | df["primary_diagnosis_code"].str.startswith("O84")])
partos_totais = partos_normais + cesarianas
taxa_cesariana = (cesarianas / max(1, partos_totais)) * 100
print(f"\n[3. ANALISE OBSTETRICA: TAXA DE CESARIANAS NO SUS]")
print(f"Total de Partos Mapeados: {partos_totais}")
print(f"Partos Normais: {partos_normais} ({partos_normais/max(1, partos_totais)*100:.1f}%)")
print(f"Cesarianas:     {cesarianas} ({taxa_cesariana:.1f}%) [Meta OMS recomendada: <15%]")

# --- ACHADO 4: Internacoes de Alta Permanencia (Outliers > 15 dias) ---
longa_perm = df[df["length_of_stay_days"] > 15]
print(f"\n[4. INTERNACOES PROLONGADAS / OUTLIERS DE LEITO (> 15 DIAS)]")
print(f"Total de internacoes de longa permanencia: {len(longa_perm)} ({len(longa_perm)/len(df)*100:.2f}%)")
print(f"Gasto total consumido por esses {len(longa_perm)} pacientes: R$ {longa_perm['total_cost_brl'].sum():,.2f} ({(longa_perm['total_cost_brl'].sum()/df['total_cost_brl'].sum())*100:.1f}% de todo o orcamento do estado)")

top_doencas_longa = longa_perm.groupby(["primary_diagnosis_code", "disease_name"]).agg(
    pacientes=("encounter_id", "count"),
    los_medio=("length_of_stay_days", "mean"),
    custo_medio=("total_cost_brl", "mean"),
    obitos=("discharge_disposition", lambda x: (x == "expired").sum())
).reset_index().sort_values("pacientes", ascending=False).head(8)
print("\nPrincipais Doencas que Reteram Leitos por Mais de 15 Dias:")
print(top_doencas_longa.to_string(index=False))

# --- ACHADO 5: Procedimentos Ambulatoriais Mais Realizados (SIA) ---
print("\n[5. TOP PROCEDIMENTOS AMBULATORIAIS MAIS EXECUTADOS NO ESTADO (SIA)]")
top_sia = sia_df["PA_PROC_ID"].value_counts().head(10).reset_index()
top_sia.columns = ["PA_PROC_ID", "total_executado"]
top_sia["nome_procedimento"] = top_sia["PA_PROC_ID"].apply(resolver_nome_procedimento)
print(top_sia.to_string(index=False))

# --- ACHADO 6: Faixas Etarias com Maior Custo e Mortalidade ---
# Calcular idade estimada aproximada ou faixas
if "IDADE" in df.columns:
    df["idade_num"] = pd.to_numeric(df["IDADE"], errors="coerce")
    df["faixa_etaria"] = pd.cut(df["idade_num"], bins=[0, 12, 19, 59, 120], labels=["0-12 anos (Pediatria)", "13-19 anos (Adolescentes)", "20-59 anos (Adultos)", "60+ anos (Idosos)"])
    faixa_stats = df.groupby("faixa_etaria").agg(
        total_internacoes=("encounter_id", "count"),
        obitos=("discharge_disposition", lambda x: (x == "expired").sum()),
        gasto_total=("total_cost_brl", "sum"),
        custo_medio=("total_cost_brl", "mean"),
        los_medio=("length_of_stay_days", "mean")
    ).reset_index()
    faixa_stats["mortalidade_pct"] = (faixa_stats["obitos"] / faixa_stats["total_internacoes"]) * 100
    print("\n[6. ANALISE DEMOGRAFICA POR FAIXA ETARIA]")
    print(faixa_stats.to_string(index=False))
