"""
Script de Exportacao do Lakehouse para SQLite e CSVs com Nomes Legiveis Completos.
Enriquece todas as tabelas com nomes de doencas (CID-10), procedimentos (SIGTAP),
municipios (IBGE), hospitais (CNES), regulacao (SISREG) e saude suplementar (ANS).
"""
import os
import sys
import sqlite3
import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.silver.terminology_names import (
    resolver_nome_doenca,
    resolver_nome_procedimento,
    resolver_nome_municipio,
    resolver_nome_hospital
)

export_dir = os.path.join(PROJECT_ROOT, "exports")
os.makedirs(export_dir, exist_ok=True)
csv_dir = os.path.join(export_dir, "csv")
os.makedirs(csv_dir, exist_ok=True)

sqlite_path = os.path.join(export_dir, "qimed_health_lakehouse.db")

print("=" * 80)
print("  EXPORTACAO DO LAKEHOUSE COM NOMES LEGIVEIS PARA SQLITE E CSVs")
print("=" * 80)

conn = sqlite3.connect(sqlite_path)

# --- 1. Enriquecer dim_patients ---
silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
p_pat = os.path.join(silver_base, "dim_patients")
if os.path.exists(p_pat):
    df_pat = DeltaTable(p_pat).to_pandas()
    df_pat["municipality_name"] = df_pat["municipality_code"].apply(resolver_nome_municipio)
    df_pat["gender_name"] = df_pat["gender"].map({"female": "Feminino", "male": "Masculino", "other": "Outro", "unknown": "Não Informado"}).fillna("Não Informado")
    df_pat.to_sql("dim_patients", conn, if_exists="replace", index=False)
    df_pat.to_csv(os.path.join(csv_dir, "dim_patients.csv"), index=False)
    print(f"-> dim_patients:         {len(df_pat):>6} linhas | + municipality_name, gender_name")

# --- 2. Enriquecer fct_encounters ---
p_enc = os.path.join(silver_base, "fct_encounters")
if os.path.exists(p_enc):
    df_enc = DeltaTable(p_enc).to_pandas()
    df_enc["hospital_name"] = df_enc["organization_id"].apply(resolver_nome_hospital)
    df_enc["primary_diagnosis_name"] = df_enc.apply(lambda r: resolver_nome_doenca(r["primary_diagnosis_code"], r.get("primary_diagnosis_chapter", "")), axis=1)
    df_enc["primary_procedure_name"] = df_enc["primary_procedure_code"].apply(resolver_nome_procedimento)
    df_enc["discharge_disposition_name"] = df_enc["discharge_disposition"].map({"discharged_alive": "Alta Hospitalar com Vida", "expired": "Óbito Hospitalar"}).fillna("Em Tratamento")
    df_enc["encounter_class_name"] = df_enc["encounter_class"].map({"EMER": "Urgência / Emergência", "IMP": "Internação Eletiva", "AMB": "Ambulatorial"}).fillna("Outros")
    df_enc.to_sql("fct_encounters", conn, if_exists="replace", index=False)
    df_enc.to_csv(os.path.join(csv_dir, "fct_encounters.csv"), index=False)
    print(f"-> fct_encounters:       {len(df_enc):>6} linhas | + hospital_name, primary_diagnosis_name, primary_procedure_name, discharge_disposition_name")

# --- 3. Enriquecer fct_conditions ---
p_cond = os.path.join(silver_base, "fct_conditions")
if os.path.exists(p_cond):
    df_cond = DeltaTable(p_cond).to_pandas()
    df_cond["disease_name"] = df_cond.apply(lambda r: resolver_nome_doenca(r["code"], r.get("chapter_description", "")), axis=1)
    df_cond["diagnosis_rank_name"] = df_cond["diagnosis_rank"].map({"primary": "Diagnóstico Principal", "secondary": "Comorbidade Secundária"}).fillna("Não Informado")
    df_cond.to_sql("fct_conditions", conn, if_exists="replace", index=False)
    df_cond.to_csv(os.path.join(csv_dir, "fct_conditions.csv"), index=False)
    print(f"-> fct_conditions:       {len(df_cond):>6} linhas | + disease_name, diagnosis_rank_name")

# --- 4. Enriquecer fct_procedures ---
p_proc = os.path.join(silver_base, "fct_procedures")
if os.path.exists(p_proc):
    df_proc = DeltaTable(p_proc).to_pandas()
    df_proc["procedure_name"] = df_proc["code"].apply(resolver_nome_procedimento)
    df_proc.to_sql("fct_procedures", conn, if_exists="replace", index=False)
    df_proc.to_csv(os.path.join(csv_dir, "fct_procedures.csv"), index=False)
    print(f"-> fct_procedures:       {len(df_proc):>6} linhas | + procedure_name")

# --- 5. Enriquecer fct_referrals (SISREG / CROSS) ---
p_ref = os.path.join(silver_base, "fct_referrals")
if os.path.exists(p_ref):
    df_ref = DeltaTable(p_ref).to_pandas()
    df_ref.to_sql("fct_referrals", conn, if_exists="replace", index=False)
    df_ref.to_csv(os.path.join(csv_dir, "fct_referrals.csv"), index=False)
    print(f"-> fct_referrals:        {len(df_ref):>6} linhas | + wait_time_days, request_hospital_name, executing_hospital_name")

# --- 6. Enriquecer dim_health_plans (ANS / D-TISS) ---
p_ans = os.path.join(silver_base, "dim_health_plans")
if os.path.exists(p_ans):
    df_ans = DeltaTable(p_ans).to_pandas()
    df_ans.to_sql("dim_health_plans", conn, if_exists="replace", index=False)
    df_ans.to_csv(os.path.join(csv_dir, "dim_health_plans.csv"), index=False)
    print(f"-> dim_health_plans:     {len(df_ans):>6} linhas | + private_coverage_ratio_pct, active_beneficiaries, loss_ratio_pct")

# --- 7. Enriquecer cnes_estabelecimentos ---
bronze_base = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "datasus")
p_cnes = os.path.join(bronze_base, "cnes")
if os.path.exists(p_cnes):
    df_cnes = DeltaTable(p_cnes).to_pandas()
    df_cnes["hospital_name"] = df_cnes["CNES"].apply(resolver_nome_hospital)
    df_cnes["municipality_name"] = df_cnes["CODUFMUN"].apply(resolver_nome_municipio)
    for col in df_cnes.columns:
        if df_cnes[col].dtype == "object":
            df_cnes[col] = df_cnes[col].astype(str)
    df_cnes.to_sql("cnes_estabelecimentos", conn, if_exists="replace", index=False)
    df_cnes.to_csv(os.path.join(csv_dir, "cnes_estabelecimentos.csv"), index=False)
    print(f"-> cnes_estabelecimentos: {len(df_cnes):>6} linhas | + hospital_name, municipality_name")

# --- 8. Enriquecer sia_ambulatorial ---
p_sia = os.path.join(bronze_base, "sia")
if os.path.exists(p_sia):
    df_sia = DeltaTable(p_sia).to_pandas()
    if "PA_PROC_ID" in df_sia.columns:
        df_sia["procedure_name"] = df_sia["PA_PROC_ID"].apply(resolver_nome_procedimento)
    if "PA_CODUNI" in df_sia.columns:
        df_sia["hospital_name"] = df_sia["PA_CODUNI"].apply(resolver_nome_hospital)
    if "PA_UFMUN" in df_sia.columns:
        df_sia["municipality_name"] = df_sia["PA_UFMUN"].apply(resolver_nome_municipio)
    for col in df_sia.columns:
        if df_sia[col].dtype == "object":
            df_sia[col] = df_sia[col].astype(str)
    df_sia.to_sql("sia_ambulatorial", conn, if_exists="replace", index=False)
    df_sia.to_csv(os.path.join(csv_dir, "sia_ambulatorial.csv"), index=False)
    print(f"-> sia_ambulatorial:     {len(df_sia):>6} linhas | + procedure_name, hospital_name, municipality_name")

# --- 9. Enriquecer sinan_agravos_dengue ---
p_sinan = os.path.join(bronze_base, "sinan")
if os.path.exists(p_sinan):
    df_sinan = DeltaTable(p_sinan).to_pandas()
    df_sinan["disease_name"] = "Dengue / Arbovirose"
    df_sinan["municipality_name"] = df_sinan["ID_MUNICIP"].apply(resolver_nome_municipio)
    for col in df_sinan.columns:
        if df_sinan[col].dtype == "object":
            df_sinan[col] = df_sinan[col].astype(str)
    df_sinan.to_sql("sinan_agravos_dengue", conn, if_exists="replace", index=False)
    df_sinan.to_csv(os.path.join(csv_dir, "sinan_agravos_dengue.csv"), index=False)
    print(f"-> sinan_agravos_dengue: {len(df_sinan):>6} linhas | + disease_name, municipality_name")

# --- 10. Enriquecer sisab_atencao_primaria ---
p_sisab = os.path.join(bronze_base, "sisab")
if os.path.exists(p_sisab):
    df_sisab = DeltaTable(p_sisab).to_pandas()
    df_sisab["municipality_name"] = df_sisab["CO_MUNICIPIO_IBGE"].apply(resolver_nome_municipio)
    for col in df_sisab.columns:
        if df_sisab[col].dtype == "object":
            df_sisab[col] = df_sisab[col].astype(str)
    df_sisab.to_sql("sisab_atencao_primaria", conn, if_exists="replace", index=False)
    df_sisab.to_csv(os.path.join(csv_dir, "sisab_atencao_primaria.csv"), index=False)
    print(f"-> sisab_atencao_primaria:{len(df_sisab):>6} linhas | + municipality_name")

conn.close()

db_size_mb = round(os.path.getsize(sqlite_path) / (1024 * 1024), 2)
print("=" * 80)
print(f"Exportacao concluida com enriquecimento de nomes legiveis!")
print(f"Banco SQLite atualizado: {sqlite_path} ({db_size_mb} MB)")
print(f"Diretorio de CSVs:       {csv_dir}")
print("=" * 80)
