"""
Busca por trajetoria sequencial exata: Hospital 1 (Alta/Transferencia) -> Hospital 2 (Obito).
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

expired_encs = enc_df[enc_df["discharge_disposition"] == "expired"]
pids = expired_encs["patient_master_id"].unique()

for pid in pids:
    sub = enc_df[enc_df["patient_master_id"] == pid].sort_values("period_start")
    if len(sub) > 1 and sub["organization_id"].nunique() > 1:
        if sub.iloc[-1]["discharge_disposition"] == "expired" and sub.iloc[0]["discharge_disposition"] == "discharged_alive":
            pat_info = pat_df[pat_df["patient_master_id"] == pid].iloc[0]
            print("\n" + "=" * 85)
            print("TRAJETORIA CRITICA ENCONTRADA: TRANSFERENCIA INTER-HOSPITALAR EVOLUINDO PARA OBITO")
            print(f"Master Patient ID: {pid}")
            print(f"Genero: {pat_info.get('gender')} | Estado: {pat_info.get('state')} | Municipio IBGE: {pat_info.get('municipality_code')}")
            print("=" * 85)
            for idx, (_, row) in enumerate(sub.iterrows(), 1):
                c = cond_df[cond_df["encounter_id"] == row["encounter_id"]]
                c_desc = [f"[{x['code']}] {x['chapter_description']}" for _, x in c.iterrows() if str(x["code"]) != "0000"]
                p = proc_df[proc_df["encounter_id"] == row["encounter_id"]]
                p_desc = [f"[{x['formatted_code']}] {x['group_description']}" for _, x in p.iterrows()]
                
                is_dead = row["discharge_disposition"] == "expired"
                tag = ">>> EVOLUCAO PARA OBITO <<<" if is_dead else "ESTABILIZACAO / ENCAMINHAMENTO"
                
                print(f"\n[Etapa {idx} da Jornada: Hospital {row['organization_id']}] ({tag})")
                print(f"  Periodo:               {row['period_start']} ate {row['period_end']} ({row['length_of_stay_days']} dias)")
                print(f"  Desfecho:              {row['discharge_disposition']}")
                print(f"  Custo SUS:             R$ {row['total_cost_brl']:.2f}")
                print(f"  Diagnosticos:          {', '.join(c_desc)}")
                print(f"  Procedimentos:         {', '.join(p_desc)}")
            break
