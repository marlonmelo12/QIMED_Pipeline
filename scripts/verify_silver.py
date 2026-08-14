"""
Script de validacao e inspecao das tabelas da Camada Silver (Delta Lake).
"""
import os
import sys
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
tables = ["dim_patients", "dim_organizations", "fct_encounters", "fct_conditions", "fct_procedures"]

print("=" * 65)
print("  VALIDACAO DAS TABELAS CANONICAS - CAMADA SILVER")
print("=" * 65)

for t in tables:
    p = os.path.join(base, t)
    if os.path.exists(p):
        dt = DeltaTable(p)
        df = dt.to_pandas()
        print(f"\nTabela: {t}")
        print(f"   Registros: {len(df)}")
        print(f"   Colunas ({len(df.columns)}): {list(df.columns)}")
        if "patient_master_id" in df.columns and not df.empty:
            print(f"   Amostra MPI (Master Patient ID): {df['patient_master_id'].iloc[0]}")
        if "chapter" in df.columns and not df.empty:
            print(f"   Capitulo CID-10: {df['chapter'].iloc[0]} - {df['chapter_description'].iloc[0]}")
        if "group_description" in df.columns and not df.empty:
            print(f"   Grupo SIGTAP: {df['group_description'].iloc[0]}")

print("\n" + "=" * 65)
print("  Inspecao da Camada Silver concluida.")
print("=" * 65)
