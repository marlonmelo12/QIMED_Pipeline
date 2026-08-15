"""
Cruzamento de Dados Multi-Fonte (SIH + CNES + SIA + SINAN + SISAB) - Acre 2025-01:
Analise Integrada de Vigilancia, Atencao Primaria, Atendimento Ambulatorial,
Capacidade Hospitalar e Faturamento de Internacoes.
"""
import os
import sys
import pandas as pd
import numpy as np
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
bronze_base = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "datasus")

# 1. Carregar Dados da Silver (SIH e Dimensoes)
enc_df = DeltaTable(os.path.join(silver_base, "fct_encounters")).to_pandas()
pat_df = DeltaTable(os.path.join(silver_base, "dim_patients")).to_pandas()
cond_df = DeltaTable(os.path.join(silver_base, "fct_conditions")).to_pandas()

# 2. Carregar Dados da Bronze (CNES, SIA, SINAN, SISAB)
cnes_df = DeltaTable(os.path.join(bronze_base, "cnes")).to_pandas()
sia_df = DeltaTable(os.path.join(bronze_base, "sia")).to_pandas()
sinan_df = DeltaTable(os.path.join(bronze_base, "sinan")).to_pandas()
sisab_df = DeltaTable(os.path.join(bronze_base, "sisab")).to_pandas()

print("=" * 85)
print("  VOLUMETRIA INTEGRADA DO ECOSSISTEMA QIMED (ACRE - JANEIRO/2025)")
print("=" * 85)
print(f"1. SIH   (Internacoes Hospitalares):          {len(enc_df):,} registros")
print(f"2. SIA   (Procedimentos Ambulatoriais):      {len(sia_df):,} registros")
print(f"3. CNES  (Cadastro de Hospitais e Leitos):    {len(cnes_df):,} estabelecimentos")
print(f"4. SINAN (Notificacoes Epidemiologicas):      {len(sinan_df):,} agravos")
print(f"5. SISAB (Producao da Atencao Primaria APS):  {len(sisab_df):,} municipios monitorados")
print("=" * 85)

# --- INFERENCIA 1: Piramide do Cuidado (SISAB -> SIA -> SIH) ---
total_consultas_aps = sisab_df["QT_ATENDIMENTOS"].sum()
total_visitas_aps = sisab_df["QT_VISITAS_DOMICILIARES"].sum()
total_procedimentos_ambulatoriais = len(sia_df)
total_valor_ambulatorial = sia_df["PA_VALPRO"].sum()
total_internacoes = len(enc_df)
total_valor_hospitalar = enc_df["total_cost_brl"].sum()

print("\n[INFERENCIA 1: PIRAMIDE ASSISTENCIAL E FLUXO DE GASTOS EM SAUDE]")
print(f"  Atenção Primária (SISAB):       {total_consultas_aps:,} consultas médicas APS + {total_visitas_aps:,} visitas domiciliares")
print(f"  Média/Alta Complexidade (SIA):   {total_procedimentos_ambulatoriais:,} procedimentos ambulatoriais faturados (R$ {total_valor_ambulatorial:,.2f})")
print(f"  Internação Hospitalar (SIH):     {total_internacoes:,} internações hospitalares faturadas (R$ {total_valor_hospitalar:,.2f})")
print(f"  -> Razão Ambulatorial/Internação: {total_procedimentos_ambulatoriais / total_internacoes:.1f} procedimentos ambulatoriais para cada 1 internação")
print(f"  -> Custo Hospitalar vs Ambulatorial: O custo hospitalar (R$ {total_valor_hospitalar:,.2f}) representa {(total_valor_hospitalar/(total_valor_hospitalar+total_valor_ambulatorial))*100:.1f}% do gasto assistencial total.")

# --- INFERENCIA 2: Taxa de Ocupacao e Pressao sobre Leitos (SIH x CNES) ---
print("\n[INFERENCIA 2: TAXA DE OCUPAÇÃO DE LEITOS POR HOSPITAL (SIH x CNES)]")
enc_hosp = enc_df.groupby("organization_id").agg(
    internacoes=("encounter_id", "count"),
    dias_leito_ocupados=("length_of_stay_days", "sum"),
    obitos=("discharge_disposition", lambda x: (x == "expired").sum()),
    custo_total=("total_cost_brl", "sum")
).reset_index()

cnes_clean = cnes_df[["CNES", "NOME_FANTASIA", "LEITOS", "LEITOS_UTI"]].drop_duplicates("CNES")
cnes_clean["organization_id"] = "org_cnes_" + cnes_clean["CNES"].astype(str)

hosp_joined = enc_hosp.merge(cnes_clean, on="organization_id", how="inner")
dias_no_mes = 31
hosp_joined["leitos_dias_disponiveis"] = hosp_joined["LEITOS"] * dias_no_mes
hosp_joined["taxa_ocupacao_pct"] = (hosp_joined["dias_leito_ocupados"] / hosp_joined["leitos_dias_disponiveis"]) * 100
hosp_joined["giro_leito"] = hosp_joined["internacoes"] / hosp_joined["LEITOS"]

cols_show = ["NOME_FANTASIA", "LEITOS", "internacoes", "dias_leito_ocupados", "taxa_ocupacao_pct", "giro_leito", "obitos"]
print(hosp_joined[cols_show].to_string(index=False))

# --- INFERENCIA 3: Vigilancia Integrada (SINAN x SIA x SIH) - O Funil da Dengue ---
print("\n[INFERENCIA 3: O FUNIL EPIDEMIOLÓGICO DA DENGUE (SINAN x SIA x SIH)]")
dengue_sih = enc_df[enc_df["primary_diagnosis_code"].str.startswith("A90") | enc_df["primary_diagnosis_code"].str.startswith("A91")]
dengue_sih_count = len(dengue_sih)
dengue_sih_custo = dengue_sih["total_cost_brl"].sum()
dengue_sih_dias = dengue_sih["length_of_stay_days"].sum()

# Exames laboratoriais de dengue no SIA (0202... ou sorologia)
sia_lab = sia_df[sia_df["PA_PROC_ID"].str.startswith("0202") | sia_df["PA_PROC_ID"].str.startswith("0201")]
sia_lab_count = len(sia_lab)

print(f"  1. Notificações Epidemiológicas (SINAN): {len(sinan_df)} casos notificados")
print(f"  2. Exames Diagnósticos Ambulatoriais (SIA): {sia_lab_count:,} exames laboratoriais executados")
print(f"  3. Internações Hospitalares por Dengue (SIH): {dengue_sih_count} pacientes internados")
print(f"     - Dias totais de leito consumidos por Dengue: {dengue_sih_dias} dias")
print(f"     - Custo total de internações por Dengue ao SUS: R$ {dengue_sih_custo:,.2f}")
print(f"     - Taxa de Hospitalização da Dengue: {(dengue_sih_count / max(1, len(sinan_df))) * 100:.1f}%")

# --- INFERENCIA 4: Efetividade da Atencao Primaria vs Internacoes Evitaveis (SISAB x SIH) ---
print("\n[INFERENCIA 4: IMPACTO DA ATENÇÃO BÁSICA NAS INTERNAÇÕES (SISAB x SIH)]")
# Internacoes por Condicoes Sensiveis a Atencao Primaria (ICSAP): Hipertensao (I10-I15), Diabetes (E10-E14), Asma (J45)
icsap_codes = ["I10", "I11", "I15", "E10", "E11", "E14", "J45"]
icsap_df = enc_df[enc_df["primary_diagnosis_code"].str[:3].isin(icsap_codes)]
icsap_count = len(icsap_df)
icsap_custo = icsap_df["total_cost_brl"].sum()

print(f"  Acompanhamento Preventivo na APS (SISAB): {sisab_df['QT_HIPERTENSOS_ACOMPANHADOS'].sum():,} hipertensos e {sisab_df['QT_DIABETICOS_ACOMPANHADOS'].sum():,} diabéticos ativos nas UBS.")
print(f"  Internações por Condições Sensíveis à Atenção Primária (ICSAP no SIH): {icsap_count} internações ({icsap_count/len(enc_df)*100:.2f}% do total)")
print(f"  Custo Hospitalar Evitável: R$ {icsap_custo:,.2f} faturados por descompensações crônicas no hospital.")

print("=" * 85)
