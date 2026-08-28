"""
Filtra um paciente com reinternacoes sequenciais mensais claras (Jan, Fev, Mar 2025).
"""
import os
import sys
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

def find_specific_patient():
    silver_path = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
    df_enc = DeltaTable(os.path.join(silver_path, "fct_encounters")).to_pandas()
    df_pat = DeltaTable(os.path.join(silver_path, "dim_patients")).to_pandas()
    
    # Agrupar por paciente e contar quantas internacoes distintas
    p_counts = df_enc.groupby("patient_master_id").agg(
        total_enc=("encounter_id", "nunique"),
        first_in=("period_start", "min"),
        last_in=("period_start", "max")
    )
    
    # Selecionar paciente com 3 a 5 internacoes espalhadas no trimestre
    candidates = p_counts[(p_counts["total_enc"] >= 3) & (p_counts["total_enc"] <= 6) & (p_counts["first_in"] != p_counts["last_in"])]
    
    target_id = candidates.index[0]
    
    pat_info = df_pat[df_pat["patient_master_id"] == target_id].iloc[0]
    encs = df_enc[df_enc["patient_master_id"] == target_id].drop_duplicates("encounter_id").sort_values("period_start")
    
    print("=" * 85)
    print(f"  PACIENTE RASTREADO: MPI [{target_id}]")
    print(f"  Gênero: {pat_info.get('gender', 'N/A')} | Município: {resolver_nome_municipio(pat_info.get('municipality_code'))} ({pat_info.get('municipality_code')})")
    print(f"  Total de Internações Únicas no Trimestre: {len(encs)}")
    print("=" * 85)
    
    for i, (_, row) in enumerate(encs.iterrows(), 1):
        print(f"\n[INTERNAÇÃO {i}] {row['period_start']} a {row['period_end']} ({row['length_of_stay_days']} dias de internação)")
        print(f"  -> Hospital:   {resolver_nome_hospital(row['organization_id'])} (CNES: {row['organization_id']})")
        print(f"  -> Diagnóstico: [{row['primary_diagnosis_code']}] {resolver_nome_doenca(row['primary_diagnosis_code'])}")
        print(f"  -> Procedimento: [{row['primary_procedure_code']}] {resolver_nome_procedimento(row['primary_procedure_code'])}")
        print(f"  -> Custo SUS:   R$ {float(row['total_cost_brl']):,.2f}")
        print(f"  -> Desfecho:    {'Alta Hospitalar com Vida' if row['discharge_disposition'] == 'discharged_alive' else row['discharge_disposition']}")

if __name__ == "__main__":
    find_specific_patient()
