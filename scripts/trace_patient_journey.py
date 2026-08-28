"""
Rastreamento Longitudinal de Paciente no 1o Trimestre de 2025 (Ceara - SIH/SUS).
Busca pacientes que tiveram reinternacoes / multiplas passagens no sistema hospitalar
ao longo de Janeiro, Fevereiro e Marco de 2025 usando o Master Patient Index (MPI).
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

def trace_patient_journey():
    silver_path = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
    
    df_encounters = DeltaTable(os.path.join(silver_path, "fct_encounters")).to_pandas()
    df_patients = DeltaTable(os.path.join(silver_path, "dim_patients")).to_pandas()
    df_conditions = DeltaTable(os.path.join(silver_path, "fct_conditions")).to_pandas()
    df_procedures = DeltaTable(os.path.join(silver_path, "fct_procedures")).to_pandas()

    print("=" * 90)
    print("  RASTREAMENTO LONGITUDINAL DE PACIENTE MULTI-INTERNADO (CEARA Q1 2025)")
    print("=" * 90)

    # Identificar pacientes com maior numero de internacoes no trimestre
    encounter_counts = df_encounters["patient_master_id"].value_counts()
    multi_patients = encounter_counts[encounter_counts >= 3]
    
    print(f"Total de pacientes identificados no MPI: {len(encounter_counts):,}")
    print(f"Pacientes com 2+ internações no trimestre: {(encounter_counts >= 2).sum():,}")
    print(f"Pacientes com 3+ internações no trimestre: {len(multi_patients):,}")

    if len(multi_patients) == 0:
        target_pid = encounter_counts.index[0]
    else:
        target_pid = multi_patients.index[0]

    # Dados demograficos do paciente
    pat_info = df_patients[df_patients["patient_master_id"] == target_pid]
    
    # Jornada de internacoes ordenadas por data
    pat_encounters = df_encounters[df_encounters["patient_master_id"] == target_pid].copy()
    pat_encounters["start_date"] = pd.to_datetime(pat_encounters["period_start"], errors="coerce")
    pat_encounters = pat_encounters.sort_values("start_date")

    print("\n" + "-" * 90)
    print(f"FICHA DO PACIENTE: MPI [{target_pid}]")
    if not pat_info.empty:
        p_row = pat_info.iloc[0]
        mun_nome = resolver_nome_municipio(p_row.get("municipality_code", ""))
        print(f"Gênero: {p_row.get('gender', 'N/A')} | Município de Residência: {mun_nome} ({p_row.get('municipality_code')})")
    print(f"Total de Internações no Trimestre (Jan-Mar 2025): {len(pat_encounters)} internações")
    print("-" * 90)

    print("\nLINHA DO TEMPO CLÍNICA LONGITUDINAL (JORNADA DO PACIENTE):")
    for idx, (_, enc) in enumerate(pat_encounters.iterrows(), 1):
        enc_id = enc["encounter_id"]
        dt_in = enc.get("period_start", "N/A")
        dt_out = enc.get("period_end", "N/A")
        hosp_cod = enc.get("organization_id", "")
        hosp_nome = resolver_nome_hospital(hosp_cod)
        diag_cod = enc.get("primary_diagnosis_code", "")
        diag_nome = resolver_nome_doenca(diag_cod)
        proc_cod = enc.get("primary_procedure_code", "")
        proc_nome = resolver_nome_procedimento(proc_cod)
        custo = float(enc.get("total_cost_brl", 0.0))
        los = enc.get("length_of_stay_days", 1)
        desfecho = "Alta com Vida" if enc.get("discharge_disposition") == "discharged_alive" else enc.get("discharge_disposition")

        # Comorbidades secundarias nessa internacao
        conds = df_conditions[df_conditions["encounter_id"] == enc_id]
        sec_diags = []
        for _, c in conds.iterrows():
            if c.get("diagnosis_rank") == "secondary":
                sec_diags.append(resolver_nome_doenca(c.get("code", "")))

        print(f"\n[EVENTO #{idx}] Internação ID: {enc_id}")
        print(f"  * Período: {dt_in} até {dt_out} (Permanência: {los} dias)")
        print(f"  * Hospital / Unidade: {hosp_nome} (CNES: {hosp_cod})")
        print(f"  * Diagnóstico Principal: [{diag_cod}] {diag_nome}")
        if sec_diags:
            print(f"  * Comorbidades Secundárias: {', '.join(sec_diags[:3])}")
        print(f"  * Procedimento Realizado: [{proc_cod}] {proc_nome}")
        print(f"  * Custo Faturado SUS: R$ {custo:,.2f}")
        print(f"  * Desfecho: {desfecho}")

    print("\n" + "=" * 90)

if __name__ == "__main__":
    trace_patient_journey()
