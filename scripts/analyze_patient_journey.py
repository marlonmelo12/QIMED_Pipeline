"""
Analise de Trajetoria de Pacientes no Delta Lake Silver.
Localiza pacientes com transito entre multiplos hospitais distintos.
"""
import os
import sys
import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
enc_df = DeltaTable(os.path.join(silver_base, "fct_encounters")).to_pandas()
pat_df = DeltaTable(os.path.join(silver_base, "dim_patients")).to_pandas()
cond_df = DeltaTable(os.path.join(silver_base, "fct_conditions")).to_pandas()
proc_df = DeltaTable(os.path.join(silver_base, "fct_procedures")).to_pandas()

counts = enc_df.groupby("patient_master_id").agg(
    total_encounters=("encounter_id", "count"),
    unique_hospitals=("organization_id", "nunique"),
    hospitals_list=("organization_id", lambda x: list(x.unique()))
).reset_index()

multi_hosp = counts[counts["unique_hospitals"] > 1].sort_values("total_encounters", ascending=False)
print(f"Total de pacientes com multiplos hospitais: {len(multi_hosp)}")

# Selecionar os primeiros 2 pacientes para exibicao detalhada
for rank in range(min(2, len(multi_hosp))):
    target_pid = multi_hosp.iloc[rank]["patient_master_id"]
    pat_info = pat_df[pat_df["patient_master_id"] == target_pid].iloc[0]
    pat_encs = enc_df[enc_df["patient_master_id"] == target_pid].sort_values("period_start")

    print("\n" + "=" * 85)
    print(f"CASO #{rank + 1} - JORNADA DO PACIENTE: {target_pid}")
    print(f"Genero: {pat_info.get('gender')} | Estado: {pat_info.get('state')} | Municipio IBGE: {pat_info.get('municipality_code')}")
    print(f"Total de Internacoes: {len(pat_encs)} | Hospitais Distintos: {pat_encs['organization_id'].unique().tolist()}")
    print("=" * 85)

    for idx, (_, enc) in enumerate(pat_encs.iterrows(), 1):
        eid = enc["encounter_id"]
        conds = cond_df[cond_df["encounter_id"] == eid]
        procs = proc_df[proc_df["encounter_id"] == eid]
        
        c_list = [f"[{c['code']}] {c['chapter_description']} ({c['diagnosis_rank']})" for _, c in conds.iterrows() if str(c['code']) != '0000']
        p_list = [f"[{pr['formatted_code']}] {pr['group_description']}" for _, pr in procs.iterrows()]
        
        print(f"\n[Internacao {idx}]")
        print(f"  Hospital (CNES):       {enc['organization_id']}")
        print(f"  Periodo / Permanencia: {enc['period_start']} ate {enc['period_end']} ({enc['length_of_stay_days']} dias)")
        print(f"  Classe / Carater:      {enc['encounter_class']} (Emergencia/Urgencia)")
        print(f"  Desfecho Clinico:      {enc['discharge_disposition']}")
        print(f"  Custo Total SUS:       R$ {enc['total_cost_brl']:.2f}")
        print(f"  Diagnosticos:          {', '.join(c_list) if c_list else 'Nenhum'}")
        print(f"  Procedimentos:         {', '.join(p_list) if p_list else 'Nenhum'}")
