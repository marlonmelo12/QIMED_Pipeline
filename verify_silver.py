"""
Validation script for Silver Delta Lake tables and Entity Resolution.
"""
import os
import sys
from deltalake import DeltaTable

base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lakehouse", "silver")
tables = ["dim_patients", "dim_organizations", "fct_encounters", "fct_conditions", "fct_procedures"]

print("=" * 65)
print("  SILVER LAKEHOUSE DELTA TABLES VALIDATION")
print("=" * 65)

for t in tables:
    p = os.path.join(base, t)
    dt = DeltaTable(p)
    df = dt.to_pandas()
    print(f"\n📁 Table: {t}")
    print(f"   Rows: {len(df)}")
    print(f"   Columns ({len(df.columns)}): {list(df.columns)}")
    if "patient_master_id" in df.columns and not df.empty:
        print(f"   Master Patient ID sample: {df['patient_master_id'].iloc[0]}")
    if "chapter" in df.columns and not df.empty:
        print(f"   CID-10 Chapter sample: {df['chapter'].iloc[0]} - {df['chapter_description'].iloc[0]}")
    if "group_description" in df.columns and not df.empty:
        print(f"   SIGTAP Group sample: {df['group_description'].iloc[0]}")

print("\n" + "=" * 65)
print("  All Silver Delta Tables verified successfully!")
print("=" * 65)
