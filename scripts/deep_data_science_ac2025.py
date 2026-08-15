"""
Script de Analise de Ciencia de Dados Senior para AC 2025-01 (Delta Lake Silver).
Calcula correlacoes, estatisticas descritivas, custos, taxa de mortalidade,
tempo de permanencia (LoS), distribuicao por capitulo CID-10, grupos SIGTAP e metricas de rede.
"""
import os
import sys
import pandas as pd
import numpy as np
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
enc_df = DeltaTable(os.path.join(silver_base, "fct_encounters")).to_pandas()
pat_df = DeltaTable(os.path.join(silver_base, "dim_patients")).to_pandas()
cond_df = DeltaTable(os.path.join(silver_base, "fct_conditions")).to_pandas()
proc_df = DeltaTable(os.path.join(silver_base, "fct_procedures")).to_pandas()

# Filtrar para Janeiro de 2025
ac_encs = enc_df[enc_df["_source_file"].str.contains("2501", na=False)].copy()
print("=" * 80)
print(f"TOTAL DE INTERNACOES AC JANEIRO 2025: {len(ac_encs)}")
print(f"PACIENTES UNICOS (MPI): {ac_encs['patient_master_id'].nunique()}")
print("=" * 80)

# 1. Metricas Globais Financeiras e Operacionais
total_custo = ac_encs["total_cost_brl"].sum()
custo_medio = ac_encs["total_cost_brl"].mean()
custo_mediano = ac_encs["total_cost_brl"].median()
los_medio = ac_encs["length_of_stay_days"].mean()
los_mediano = ac_encs["length_of_stay_days"].median()
obitos = (ac_encs["discharge_disposition"] == "expired").sum()
taxa_mortalidade = (obitos / len(ac_encs)) * 100

print(f"\n[1. METRICAS GLOBAIS]")
print(f"  Custo Total Faturado SUS:      R$ {total_custo:,.2f}")
print(f"  Custo Medio por Internacao:    R$ {custo_medio:,.2f} (Mediana: R$ {custo_mediano:,.2f})")
print(f"  Tempo Medio de Permanencia:    {los_medio:.2f} dias (Mediana: {los_mediano:.1f} dias)")
print(f"  Total de Obitos Hospitalares:  {obitos} ({taxa_mortalidade:.2f}%)")

# 2. Distribuicao por Carater de Atendimento (Eletivo vs Urgencia)
carater = ac_encs["encounter_class"].value_counts(normalize=True) * 100
print(f"\n[2. CARATER DE ATENDIMENTO]")
for k, v in carater.items():
    print(f"  {k}: {v:.2f}%")

# 3. Top 10 Capitulos da CID-10 (Carga de Doenca)
capitulos = ac_encs["primary_diagnosis_chapter"].value_counts().head(10)
print(f"\n[3. TOP CAPITULOS CID-10 MAIS FREQUENTES]")
print(capitulos)

# 4. Top Diagnosticos com Maior Taxa de Mortalidade
diag_morte = ac_encs.groupby(["primary_diagnosis_code", "primary_diagnosis_chapter"]).agg(
    total=("encounter_id", "count"),
    obitos=("discharge_disposition", lambda x: (x == "expired").sum()),
    custo_medio=("total_cost_brl", "mean"),
    los_medio=("length_of_stay_days", "mean")
).reset_index()
diag_morte["taxa_morte_pct"] = (diag_morte["obitos"] / diag_morte["total"]) * 100
top_letais = diag_morte[diag_morte["total"] >= 10].sort_values("taxa_morte_pct", ascending=False).head(10)
print(f"\n[4. TOP DIAGNOSTICOS COM MAIOR TAXA DE LETALIDADE (Min. 10 casos)]")
print(top_letais.to_string(index=False))

# 5. Top 5 Doencas de Maior Impacto Financeiro (Custo Total Acumulado)
top_custo_cid = ac_encs.groupby("primary_diagnosis_code").agg(
    total_gasto=("total_cost_brl", "sum"),
    total_casos=("encounter_id", "count"),
    custo_medio=("total_cost_brl", "mean"),
    capitulo=("primary_diagnosis_chapter", "first")
).sort_values("total_gasto", ascending=False).head(10)
print(f"\n[5. TOP DIAGNOSTICOS POR CUSTO TOTAL ACUMULADO]")
print(top_custo_cid.to_string())

# 6. Concentracao Hospitalar (Volume e Mortalidade por CNES)
hosp_stats = ac_encs.groupby("organization_id").agg(
    total_internacoes=("encounter_id", "count"),
    total_gasto=("total_cost_brl", "sum"),
    obitos=("discharge_disposition", lambda x: (x == "expired").sum()),
    los_medio=("length_of_stay_days", "mean")
).reset_index()
hosp_stats["mortalidade_pct"] = (hosp_stats["obitos"] / hosp_stats["total_internacoes"]) * 100
hosp_stats = hosp_stats.sort_values("total_internacoes", ascending=False).head(10)
print(f"\n[6. CONCENTRACAO POR HOSPITAL (CNES)]")
print(hosp_stats.to_string(index=False))

# 7. Analise Demografica (Genero e Geografia)
merged_pat = ac_encs.merge(pat_df, on="patient_master_id", how="left")
print(f"\n[7. PERFIL DEMOGRAFICO]")
print(f"Distribuicao por Genero:")
print(merged_pat["gender"].value_counts(normalize=True) * 100)
print(f"Top 5 Municipios de Residencia (IBGE):")
print(merged_pat["municipality_code"].value_counts().head(5))

# 8. Reinternacoes no Mesmo Mes (Linkage MPI)
mpi_counts = ac_encs.groupby("patient_master_id")["encounter_id"].count()
multi_readmissions = mpi_counts[mpi_counts > 1]
print(f"\n[8. READMISSOES NO MESMO MES (MPI LINKAGE)]")
print(f"Pacientes reinternados em Jan/2025: {len(multi_readmissions)} ({len(multi_readmissions)/ac_encs['patient_master_id'].nunique()*100:.2f}%)")
print(f"Maximo de internacoes de um unico paciente no mes: {mpi_counts.max()}")
