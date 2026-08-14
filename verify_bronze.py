"""Verify the Bronze layer and catalog after ingestion."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from deltalake import DeltaTable

path = os.path.join(os.path.dirname(__file__), "lakehouse", "bronze", "fhir", "synthetic")
dt = DeltaTable(path)
df = dt.to_pandas()

print("=== BRONZE LAYER VERIFICATION ===")
print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
rt_counts = df["resourceType"].value_counts().to_dict()
print(f"Resource types: {rt_counts}")
partitions = df[["year","month"]].drop_duplicates().values.tolist()
print(f"Partitions (year/month): {partitions}")
print()

print("=== PII CHECK (should be hashed, 64 hex chars) ===")
patients = df[df["resourceType"]=="Patient"]
for col in ["name_family", "name_given", "cpf", "birthDate"]:
    if col in patients.columns:
        sample = patients[col].dropna()
        if not sample.empty:
            val = str(sample.iloc[0])
            hashed = len(val) == 64 and all(c in "0123456789abcdef" for c in val)
            print(f"  {col}: {val[:24]}... (hashed={hashed})")
print()

print("=== METADATA COLUMNS ===")
print(f"  _ingested_at: {df['_ingested_at'].iloc[0]}")
print(f"  _source_type: {df['_source_type'].iloc[0]}")
print(f"  _source_file: {df['_source_file'].iloc[0]}")
print()

catalog_path = os.path.join(os.path.dirname(__file__), "_metadata", "catalog.json")
with open(catalog_path) as f:
    catalog = json.load(f)
print("=== CATALOG ===")
for ds in catalog["datasets"]:
    print(f"  ID:             {ds['dataset_id']}")
    print(f"  Source:          {ds['source_type']}")
    print(f"  Rows:            {ds['row_count']}")
    print(f"  PII anonymized:  {ds['pii_anonymized']}")
    print(f"  Ingested at:     {ds['ingested_at']}")
