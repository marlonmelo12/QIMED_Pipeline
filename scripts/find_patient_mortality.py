"""
Busca de Paciente com Multiplas Internacoes Sucessivas e Desfecho em Obito (Morte).
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

def find_patient_with_rehospitalization_and_mortality():
    silver_path = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
    df_enc = DeltaTable(os.path.join(silver_path, "fct_encounters")).to_pandas()
    df_pat = DeltaTable(os.path.join(silver_path, "dim_patients")).to_pandas()
    df_cond = DeltaTable(os.path.join(silver_path, "fct_conditions")).to_pandas()

    print("=" * 85)
    print("  BUSCA DE PACIENTES COM INTERNACOES SUCESSIVAS E DESFECHO EM OBITO (SIH/SUS)")
    print("=" * 85)

    # Identificar pacientes que tiveram pelo menos 2 internacoes E terminaram com 'expired' na ultima internacao
    p_groups = df_enc.groupby("patient_master_id")
    
    candidates = []
    for pid, group in p_groups:
        if len(group) >= 2:
            # Ordenar por data
            group_sorted = group.sort_values("period_start")
            # Verificar se a ultima internacao foi óbito
            last_enc = group_sorted.iloc[-1]
            first_enc = group_sorted.iloc[0]
            if last_enc["discharge_disposition"] == "expired":
                candidates.append({
                    "patient_master_id": pid,
                    "enc_count": len(group),
                    "first_start": first_enc["period_start"],
                    "last_start": last_enc["period_start"],
                    "last_end": last_enc["period_end"],
                    "total_cost": group["total_cost_brl"].astype(float).sum(),
                    "total_days": group["length_of_stay_days"].astype(int).sum()
                })

    df_cand = pd.DataFrame(candidates)
    print(f"Total de pacientes com múltiplas internações culminando em Óbito: {len(df_cand):,}")

    if df_cand.empty:
        print("Nenhum candidato com óbito encontrado.")
        return

    # Escolher um paciente representativo (ex: com 2 a 4 internacoes com datas distintas)
    df_cand_distinct = df_cand[df_cand["first_start"] != df_cand["last_start"]].sort_values("enc_count", ascending=False)
    
    target_row = df_cand_distinct.iloc[0] if not df_cand_distinct.empty else df_cand.iloc[0]
    target_pid = target_row["patient_master_id"]

    pat_info = df_pat[df_pat["patient_master_id"] == target_pid].iloc[0]
    encs = df_enc[df_enc["patient_master_id"] == target_pid].drop_duplicates("encounter_id").sort_values("period_start")

    print("\n" + "=" * 85)
    print(f"  PACIENTE RASTREADO: MPI [{target_pid}]")
    print(f"  Gênero: {pat_info.get('gender', 'N/A')} | Município: {resolver_nome_municipio(pat_info.get('municipality_code'))} ({pat_info.get('municipality_code')})")
    print(f"  Total de Internações: {len(encs)} | Custo Total Acumulado: R$ {target_row['total_cost']:,.2f} | Permanência: {target_row['total_days']} dias")
    print("=" * 85)

    for i, (_, row) in enumerate(encs.iterrows(), 1):
        enc_id = row["encounter_id"]
        desfecho = "Óbito Hospitalar (Morte)" if row["discharge_disposition"] == "expired" else "Alta Hospitalar com Vida"
        print(f"\n[INTERNAÇÃO {i}] {row['period_start']} a {row['period_end']} ({row['length_of_stay_days']} dias)")
        print(f"  -> Hospital:   {resolver_nome_hospital(row['organization_id'])} (CNES: {row['organization_id']})")
        print(f"  -> Diagnóstico: [{row['primary_diagnosis_code']}] {resolver_nome_doenca(row['primary_diagnosis_code'])}")
        print(f"  -> Procedimento: [{row['primary_procedure_code']}] {resolver_nome_procedimento(row['primary_procedure_code'])}")
        print(f"  -> Custo SUS:   R$ {float(row['total_cost_brl']):,.2f}")
        print(f"  -> Desfecho Clínico: {desfecho}")

if __name__ == "__main__":
    find_patient_with_rehospitalization_and_mortality()
