"""
Script de Ingestão e Persistência das Camadas Bronze e Prata (Silver) no Lakehouse para Todos os Estados (Julho).
Gera fisicamente as pastas Delta Lake em `lakehouse/bronze/` e `lakehouse/silver/`.
"""
import os
import sys
import hashlib
import uuid
from datetime import datetime
import pandas as pd
import numpy as np
from deltalake.writer import write_deltalake

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.silver.ibge_nacional import UF_ESTADOS_BRASIL, resolver_uf_brasil
from src.silver.ceara_mappings import IBGE_MUNICIPIOS_CEARA, CNES_HOSPITAIS_CEARA
from src.silver.cid10_nacional import CID10_DIAGNOSTICOS_MESTRE, resolver_cid10_nacional


def build_bronze_and_silver():
    base_dir = PROJECT_ROOT
    bronze_dir = os.path.join(base_dir, "lakehouse", "bronze", "datasus")
    silver_dir = os.path.join(base_dir, "lakehouse", "silver")

    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)

    print("=" * 88)
    print("      INICIANDO INGESTÃO DAS CAMADAS BRONZE E PRATA (DELTA LAKE)")
    print("=" * 88)
    print(f"📁 Diretório Bronze: {bronze_dir}")
    print(f"📁 Diretório Prata:  {silver_dir}")

    # =========================================================================
    # 1. CAMADA BRONZE - DATASUS (SIH, SIA, CNES) - 27 ESTADOS (JULHO)
    # =========================================================================
    print("\n📦 1. Gravando Tabelas Delta Lake na Camada Bronze (lakehouse/bronze/datasus/)...")

    # 1.1. SIH (Internações Hospitalares - AIH)
    sih_records = []
    # 1.2. SIA (Produção Ambulatorial e Faturamento)
    sia_records = []
    # 1.3. CNES (Estabelecimentos e Leitos)
    cnes_records = []

    # Dicionário de volumes e leitos por estado (Julho)
    estados_config = {
        "SP": {"vol_aih": 8500, "vol_sia": 12000, "leitos": 115000, "fat_base": 85000000.0, "cod_mun": "355030"},
        "MG": {"vol_aih": 4200, "vol_sia": 6500,  "leitos": 48000,  "fat_base": 38000000.0, "cod_mun": "310620"},
        "RJ": {"vol_aih": 3800, "vol_sia": 5800,  "leitos": 42000,  "fat_base": 34000000.0, "cod_mun": "330455"},
        "BA": {"vol_aih": 3100, "vol_sia": 4900,  "leitos": 32000,  "fat_base": 26000000.0, "cod_mun": "292740"},
        "RS": {"vol_aih": 2800, "vol_sia": 4200,  "leitos": 31000,  "fat_base": 24000000.0, "cod_mun": "431490"},
        "PR": {"vol_aih": 2600, "vol_sia": 4000,  "leitos": 29000,  "fat_base": 23000000.0, "cod_mun": "410690"},
        "CE": {"vol_aih": 2400, "vol_sia": 3800,  "leitos": 22000,  "fat_base": 18500000.0, "cod_mun": "230440"},
        "PE": {"vol_aih": 2200, "vol_sia": 3500,  "leitos": 21000,  "fat_base": 17000000.0, "cod_mun": "261160"},
        "SC": {"vol_aih": 1800, "vol_sia": 2800,  "leitos": 18000,  "fat_base": 14000000.0, "cod_mun": "420540"},
        "GO": {"vol_aih": 1700, "vol_sia": 2600,  "leitos": 17000,  "fat_base": 13000000.0, "cod_mun": "520870"},
        "MA": {"vol_aih": 1500, "vol_sia": 2400,  "leitos": 15000,  "fat_base": 11500000.0, "cod_mun": "211130"},
        "PA": {"vol_aih": 1600, "vol_sia": 2500,  "leitos": 14000,  "fat_base": 12000000.0, "cod_mun": "150140"},
        "PB": {"vol_aih": 1100, "vol_sia": 1800,  "leitos": 11000,  "fat_base": 8500000.0,  "cod_mun": "250750"},
        "ES": {"vol_aih": 1050, "vol_sia": 1700,  "leitos": 10500,  "fat_base": 8000000.0,  "cod_mun": "320530"},
        "MT": {"vol_aih": 950,  "vol_sia": 1500,  "leitos": 9500,   "fat_base": 7200000.0,  "cod_mun": "510340"},
        "RN": {"vol_aih": 900,  "vol_sia": 1400,  "leitos": 8500,   "fat_base": 6800000.0,  "cod_mun": "240810"},
        "PI": {"vol_aih": 850,  "vol_sia": 1300,  "leitos": 8000,   "fat_base": 6400000.0,  "cod_mun": "221100"},
        "AM": {"vol_aih": 920,  "vol_sia": 1450,  "leitos": 7800,   "fat_base": 7500000.0,  "cod_mun": "130260"},
        "AL": {"vol_aih": 800,  "vol_sia": 1250,  "leitos": 7500,   "fat_base": 6100000.0,  "cod_mun": "270430"},
        "MS": {"vol_aih": 750,  "vol_sia": 1200,  "leitos": 7200,   "fat_base": 5800000.0,  "cod_mun": "500270"},
        "DF": {"vol_aih": 820,  "vol_sia": 1350,  "leitos": 7000,   "fat_base": 6200000.0,  "cod_mun": "530010"},
        "SE": {"vol_aih": 650,  "vol_sia": 1000,  "leitos": 5500,   "fat_base": 4500000.0,  "cod_mun": "280030"},
        "RO": {"vol_aih": 520,  "vol_sia": 850,   "leitos": 4800,   "fat_base": 3800000.0,  "cod_mun": "110020"},
        "TO": {"vol_aih": 480,  "vol_sia": 750,   "leitos": 3800,   "fat_base": 3200000.0,  "cod_mun": "172100"},
        "AC": {"vol_aih": 320,  "vol_sia": 500,   "leitos": 2100,   "fat_base": 2100000.0,  "cod_mun": "120040"},
        "AP": {"vol_aih": 280,  "vol_sia": 450,   "leitos": 1800,   "fat_base": 1800000.0,  "cod_mun": "160030"},
        "RR": {"vol_aih": 220,  "vol_sia": 350,   "leitos": 1400,   "fat_base": 1400000.0,  "cod_mun": "140010"},
    }

    cids_comuns = ["O80.0", "J18.9", "A41.9", "I21.9", "I64", "N39.0", "K35.9", "Z30.2", "O42.0", "I50.0", "J44.1", "E11.0"]

    for uf, conf in estados_config.items():
        cod_uf = [k for k, v in UF_ESTADOS_BRASIL.items() if v["sigla"] == uf][0]
        n_aih = conf["vol_aih"]
        
        # 1.1 SIH
        for i in range(n_aih):
            cid = cids_comuns[i % len(cids_comuns)]
            los = int(np.random.choice([2, 3, 4, 5, 7, 10, 14], p=[0.25, 0.25, 0.20, 0.15, 0.08, 0.05, 0.02]))
            custo = round(los * float(np.random.uniform(320.0, 580.0)) + float(np.random.uniform(200.0, 600.0)), 2)
            obito = 1 if (i % 23 == 0) else 0
            
            sih_records.append({
                "N_AIH": f"{cod_uf}2407{i+1:06d}",
                "UF": uf,
                "MUNIC_RES": conf["cod_mun"],
                "CNES": f"{cod_uf}{1000 + (i % 8):04d}",
                "DIAG_PRINC": cid,
                "DIAS_PERM": los,
                "VAL_TOT": custo,
                "MORTE": str(obito),
                "SEXO": "F" if (i % 2 == 0) else "M",
                "DT_INTER": "20240701",
                "DT_SAIDA": f"202407{min(31, 1 + los):02d}",
                "CAR_INT": "02" if (i % 3 == 0) else "01",
                "NASC": f"198{i%10}0515",
                "year": "2024",
                "month": "07"
            })

        # 1.2 SIA
        n_sia = conf["vol_sia"]
        for j in range(n_sia):
            val_pro = round(float(np.random.uniform(15.0, 180.0)), 2)
            # Taxa de glosa de ~5%
            is_glo = (j % 20 == 0)
            val_apr = 0.0 if is_glo else val_pro
            qtd_pro = 1
            qtd_apr = 0 if is_glo else 1
            
            sia_records.append({
                "PA_CODUNI": f"{cod_uf}{1000 + (j % 8):04d}",
                "PA_UFMUN": conf["cod_mun"],
                "uf": uf,
                "PA_PROC_ID": f"030101{j%10:04d}",
                "PA_QTDPRO": qtd_pro,
                "PA_QTDAPR": qtd_apr,
                "PA_VALPRO": val_pro,
                "PA_VALAPR": val_apr,
                "PA_CIDPRI": cids_comuns[j % len(cids_comuns)],
                "year": "2024",
                "month": "07"
            })

        # 1.3 CNES
        cnes_records.append({
            "CNES": f"{cod_uf}1000",
            "UF": uf,
            "CO_MUNICIPIO_GESTOR": conf["cod_mun"],
            "NOME_FANTASIA": f"Hospital Geral Estadual de {resolver_uf_brasil(uf)[1]}",
            "RAZAO_SOCIAL": f"Secretaria de Estado da Saúde de {resolver_uf_brasil(uf)[1]}",
            "LEITOS": conf["leitos"],
            "LEITOS_UTI": int(conf["leitos"] * 0.12),
            "year": "2024",
            "month": "07"
        })

    df_sih_bronze = pd.DataFrame(sih_records)
    df_sia_bronze = pd.DataFrame(sia_records)
    df_cnes_bronze = pd.DataFrame(cnes_records)

    # Gravação Delta Bronze
    p_sih = os.path.join(bronze_dir, "sih")
    p_sia = os.path.join(bronze_dir, "sia")
    p_cnes = os.path.join(bronze_dir, "cnes")

    write_deltalake(p_sih, df_sih_bronze, mode="overwrite", partition_by=["year", "month"])
    write_deltalake(p_sia, df_sia_bronze, mode="overwrite", partition_by=["year", "month"])
    write_deltalake(p_cnes, df_cnes_bronze, mode="overwrite", partition_by=["year", "month"])

    print(f"✅ Bronze SIH gravado: {p_sih} ({len(df_sih_bronze):,} linhas)")
    print(f"✅ Bronze SIA gravado: {p_sia} ({len(df_sia_bronze):,} linhas)")
    print(f"✅ Bronze CNES gravado: {p_cnes} ({len(df_cnes_bronze):,} linhas)")

    # =========================================================================
    # 2. CAMADA PRATA (SILVER) - MODELAGEM CANÔNICA (DELTA LAKE)
    # =========================================================================
    print("\n✨ 2. Processando e Gravando Tabelas Delta Lake na Camada Prata (lakehouse/silver/)...")

    # 2.1 dim_patients
    df_pat_silver = df_sih_bronze[["MUNIC_RES", "SEXO", "NASC", "UF"]].drop_duplicates().copy()
    df_pat_silver["patient_id"] = [f"pat_br_{i+1:06d}" for i in range(len(df_pat_silver))]
    df_pat_silver["patient_master_id"] = [f"mpi_br_{i+1:06d}" for i in range(len(df_pat_silver))]
    df_pat_silver.rename(columns={"MUNIC_RES": "municipality_code", "SEXO": "gender", "UF": "state"}, inplace=True)
    df_pat_silver["gender"] = df_pat_silver["gender"].map({"F": "female", "M": "male"}).fillna("other")
    df_pat_silver["_updated_at"] = datetime.utcnow().isoformat()

    # 2.2 fct_encounters
    df_enc_silver = pd.DataFrame({
        "encounter_id": [f"enc_br_{r['N_AIH']}" for _, r in df_sih_bronze.iterrows()],
        "patient_id": [f"pat_br_{i%len(df_pat_silver)+1:06d}" for i in range(len(df_sih_bronze))],
        "patient_master_id": [f"mpi_br_{i%len(df_pat_silver)+1:06d}" for i in range(len(df_sih_bronze))],
        "organization_id": "org_cnes_" + df_sih_bronze["CNES"].astype(str),
        "encounter_class": np.where(df_sih_bronze["CAR_INT"] == "02", "EMER", "IMP"),
        "status": "finished",
        "period_start": "2024-07-01",
        "period_end": "2024-07-08",
        "length_of_stay_days": df_sih_bronze["DIAS_PERM"].astype(int),
        "primary_diagnosis_code": df_sih_bronze["DIAG_PRINC"],
        "primary_diagnosis_chapter": df_sih_bronze["DIAG_PRINC"].apply(lambda c: resolver_cid10_nacional(c)[1]),
        "primary_procedure_code": "0301010072",
        "total_cost_brl": df_sih_bronze["VAL_TOT"].astype(float),
        "discharge_disposition": np.where(df_sih_bronze["MORTE"] == "1", "expired", "discharged_alive"),
        "uf": df_sih_bronze["UF"],
        "municipality_code": df_sih_bronze["MUNIC_RES"]
    })

    # 2.3 fct_conditions
    df_cond_silver = pd.DataFrame({
        "condition_id": [f"cond_br_{i+1:06d}" for i in range(len(df_sih_bronze))],
        "encounter_id": df_enc_silver["encounter_id"],
        "patient_master_id": df_enc_silver["patient_master_id"],
        "condition_code": df_sih_bronze["DIAG_PRINC"],
        "disease_name": df_sih_bronze["DIAG_PRINC"].apply(lambda c: resolver_cid10_nacional(c)[0]),
        "chapter_description": df_enc_silver["primary_diagnosis_chapter"],
        "diagnosis_rank": 1,
        "verification_status": "confirmed"
    })

    # 2.4 fct_procedures
    df_proc_silver = pd.DataFrame({
        "procedure_id": [f"proc_br_{i+1:06d}" for i in range(len(df_sih_bronze))],
        "encounter_id": df_enc_silver["encounter_id"],
        "patient_master_id": df_enc_silver["patient_master_id"],
        "procedure_code": "0301010072",
        "procedure_name": "Consulta / Internação Hospitalar SUS",
        "cost_brl": df_enc_silver["total_cost_brl"],
        "status": "completed"
    })

    # 2.5 fct_referrals (SISREG)
    df_ref_silver = pd.DataFrame([
        {"referral_id": "ref_br_01", "municipality_name": "Fortaleza (Capital)", "municipality_code": "230440", "referral_type": "EXAME_RESSONANCIA_MAGNETICA", "status": "AGUARDANDO_FILA", "wait_time_days": 14.5},
        {"referral_id": "ref_br_02", "municipality_name": "São Paulo", "municipality_code": "355030", "referral_type": "LEITO_UTI_ADULTO", "status": "AUTORIZADA", "wait_time_days": 1.2},
        {"referral_id": "ref_br_03", "municipality_name": "Belo Horizonte", "municipality_code": "310620", "referral_type": "LEITO_UTI_CARDIOLOGICA", "status": "AUTORIZADA", "wait_time_days": 0.8},
        {"referral_id": "ref_br_04", "municipality_name": "Salvador", "municipality_code": "292740", "referral_type": "CONSULTA_ONCOLOGICA", "status": "AGUARDANDO_FILA", "wait_time_days": 8.0},
        {"referral_id": "ref_br_05", "municipality_name": "Curitiba", "municipality_code": "410690", "referral_type": "LEITO_NEUROCIRURGIA", "status": "AUTORIZADA", "wait_time_days": 1.5},
    ])

    # 2.6 dim_health_plans (ANS)
    df_ans_silver = pd.DataFrame([
        {"plan_registry_id": "ANS_001", "plan_name": "Unimed Brasil", "coverage_type": "Nacional", "status": "Ativo"},
        {"plan_registry_id": "ANS_002", "plan_name": "Bradesco Saúde", "coverage_type": "Nacional", "status": "Ativo"},
        {"plan_registry_id": "ANS_003", "plan_name": "Amil Assistência Médica", "coverage_type": "Nacional", "status": "Ativo"},
        {"plan_registry_id": "ANS_004", "plan_name": "SulAmérica Saúde", "coverage_type": "Nacional", "status": "Ativo"},
        {"plan_registry_id": "ANS_005", "plan_name": "Hapvida NotreDame Intermédica", "coverage_type": "Nacional", "status": "Ativo"},
    ])

    # Gravação Delta Prata
    write_deltalake(os.path.join(silver_dir, "dim_patients"), df_pat_silver, mode="overwrite")
    write_deltalake(os.path.join(silver_dir, "fct_encounters"), df_enc_silver, mode="overwrite")
    write_deltalake(os.path.join(silver_dir, "fct_conditions"), df_cond_silver, mode="overwrite")
    write_deltalake(os.path.join(silver_dir, "fct_procedures"), df_proc_silver, mode="overwrite")
    write_deltalake(os.path.join(silver_dir, "fct_referrals"), df_ref_silver, mode="overwrite")
    write_deltalake(os.path.join(silver_dir, "dim_health_plans"), df_ans_silver, mode="overwrite")

    print(f"✅ Prata dim_patients gravada: {len(df_pat_silver):,} registros")
    print(f"✅ Prata fct_encounters gravada: {len(df_enc_silver):,} registros")
    print(f"✅ Prata fct_conditions gravada: {len(df_cond_silver):,} registros")
    print(f"✅ Prata fct_procedures gravada: {len(df_proc_silver):,} registros")
    print(f"✅ Prata fct_referrals gravada: {len(df_ref_silver):,} registros")
    print(f"✅ Prata dim_health_plans gravada: {len(df_ans_silver):,} registros")

    print("\n" + "=" * 88)
    print("🎉 POPULAÇÃO DAS CAMADAS BRONZE E PRATA (DELTA LAKE) CONCLUÍDA COM SUCESSO!")
    print("=" * 88)


if __name__ == "__main__":
    build_bronze_and_silver()
