"""
Script de inspecao e verificacao da Camada Bronze Delta Lake e Catalogo de Metadados.
"""
import os
import sys
import json
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

path = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "fhir", "synthetic")
if os.path.exists(path):
    dt = DeltaTable(path)
    df = dt.to_pandas()

    print("=" * 65)
    print("  VERIFICACAO DA CAMADA BRONZE (DELTA LAKE)")
    print("=" * 65)
    print(f"Total de registros: {len(df)}")
    print(f"Colunas ({len(df.columns)}): {list(df.columns)}")
    rt_counts = df["resourceType"].value_counts().to_dict()
    print(f"Distribuicao por tipo de recurso: {rt_counts}")
    partitions = df[["year", "month"]].drop_duplicates().values.tolist()
    print(f"Particoes (ano/mes): {partitions}")
    print()

    print("Verificacao de Hashing LGPD (64 caracteres hexadecimais):")
    patients = df[df["resourceType"] == "Patient"]
    for col in ["name_family", "name_given", "cpf", "birthDate"]:
        if col in patients.columns:
            sample = patients[col].dropna()
            if not sample.empty:
                val = str(sample.iloc[0])
                hashed = len(val) == 64 and all(c in "0123456789abcdef" for c in val)
                print(f"  {col}: {val[:24]}... (hash_sha256_valido={hashed})")
    print()

catalog_path = os.path.join(PROJECT_ROOT, "_metadata", "catalog.json")
if os.path.exists(catalog_path):
    with open(catalog_path) as f:
        catalog = json.load(f)
    print("Catalogo de Metadados:")
    for ds in catalog.get("datasets", []):
        print(f"  - Dataset ID:     {ds.get('dataset_id')}")
        print(f"    Fonte:          {ds.get('source_type')}")
        print(f"    Registros:      {ds.get('row_count')}")
        print(f"    Data Ingestao:  {ds.get('ingested_at')}")
