"""
Script de Exportacao do Lakehouse (Delta Lake Silver e Bronze) para SQLite e CSVs.
Gera um banco de dados relacional standalone (qimed_health_lakehouse.db)
e arquivos CSV prontos para compartilhamento e analise em Jupyter Notebooks.
"""
import os
import sys
import sqlite3
import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

export_dir = os.path.join(PROJECT_ROOT, "exports")
os.makedirs(export_dir, exist_ok=True)
csv_dir = os.path.join(export_dir, "csv")
os.makedirs(csv_dir, exist_ok=True)

sqlite_path = os.path.join(export_dir, "qimed_health_lakehouse.db")

print("=" * 75)
print("  EXPORTACAO DO LAKEHOUSE PARA SQLITE E CSVs")
print("=" * 75)

# Conectar ao SQLite
conn = sqlite3.connect(sqlite_path)

# 1. Tabelas da Camada Silver
silver_base = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
silver_tables = ["dim_patients", "dim_organizations", "fct_encounters", "fct_conditions", "fct_procedures"]

for t in silver_tables:
    p = os.path.join(silver_base, t)
    if os.path.exists(p):
        df = DeltaTable(p).to_pandas()
        # Gravar no SQLite
        df.to_sql(t, conn, if_exists="replace", index=False)
        # Gravar CSV
        csv_file = os.path.join(csv_dir, f"{t}.csv")
        df.to_csv(csv_file, index=False)
        print(f"-> Tabela Silver exportada: {t:<20} | Registros: {len(df):>6} | CSV: {os.path.basename(csv_file)}")

# 2. Tabelas da Camada Bronze Complementares (SIA, SINAN, SISAB, CNES)
bronze_base = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "datasus")
bronze_sources = [
    ("cnes", "cnes_estabelecimentos"),
    ("sia", "sia_ambulatorial"),
    ("sinan", "sinan_agravos_dengue"),
    ("sisab", "sisab_atencao_primaria")
]

for sub, name in bronze_sources:
    p = os.path.join(bronze_base, sub)
    if os.path.exists(p):
        df = DeltaTable(p).to_pandas()
        # Limpar tipos complexos caso existam para SQLite
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)
        df.to_sql(name, conn, if_exists="replace", index=False)
        csv_file = os.path.join(csv_dir, f"{name}.csv")
        df.to_csv(csv_file, index=False)
        print(f"-> Tabela Bronze exportada: {name:<20} | Registros: {len(df):>6} | CSV: {os.path.basename(csv_file)}")

conn.close()

db_size_mb = round(os.path.getsize(sqlite_path) / (1024 * 1024), 2)
print("=" * 75)
print(f"Exportacao concluida com sucesso!")
print(f"Banco SQLite gerado: {sqlite_path} ({db_size_mb} MB)")
print(f"Diretorio de CSVs:   {csv_dir}")
print("=" * 75)
