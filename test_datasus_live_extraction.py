"""
Live Extraction & End-to-End Test from DATASUS FTP (SIH & CNES).
Downloads real DBC files from ftp.datasus.gov.br, decompresses them,
applies LGPD pseudoanonimization, writes to Bronze, and normalizes into Silver Delta Lake.
"""
import os
import sys
import time

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.collectors.datasus_collector import DatasusCollector
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.validators.datasus_validator import DatasusValidator
from src.lakehouse.bronze_writer import BronzeWriter
from src.silver.pipeline import SilverTransformationPipeline
from src.utils.logging_config import get_logger

logger = get_logger("datasus_live_test")


def main():
    print("=" * 70)
    print("  QIMED DataQore — Teste de Extração Real DATASUS (FTP -> Bronze -> Silver)")
    print("=" * 70)

    # Parametrização do teste: Acre (AC) - competência 2026/01 (RDAC2601.dbc)
    subsystem = "SIH"
    uf = "AC"
    year = 2026
    month = 1

    print(f"\n[1/6] Conectando ao FTP do DATASUS (ftp.datasus.gov.br)...")
    print(f"      Subsistema: {subsystem} | UF: {uf} | Competência: {year}/{month:02d}")

    collector = DatasusCollector(subsystem=subsystem, uf=uf, year=year, month=month)
    pii_detector = PIIDetector()
    anonymizer = Anonymizer(salt="qimed_prod_salt_2026")
    validator = DatasusValidator(subsystem=subsystem)
    bronze_writer = BronzeWriter()

    # Step 1: Download DBC via FTP
    start_dl = time.time()
    dbc_file = collector.fetch()
    dl_time = round(time.time() - start_dl, 2)
    print(f"      ✅ Download concluído em {dl_time}s: {os.path.basename(dbc_file)}")

    # Step 2: Decompress DBC -> DBF and Parse
    print("\n[2/6] Descomprimindo formato proprietário DBC e extraindo DBF...")
    start_parse = time.time()
    raw_df = collector.parse(dbc_file)
    parse_time = round(time.time() - start_parse, 2)
    print(f"      ✅ Descompressão & parsing concluídos em {parse_time}s")
    print(f"      📊 Total de internações extraídas: {len(raw_df)} registros")
    print(f"      📋 Colunas disponíveis ({len(raw_df.columns)}): {list(raw_df.columns[:10])}...")

    # Step 3: LGPD Gate Detection & Masking
    print("\n[3/6] Passando pelo LGPD Gate (Detecção e Hashing SHA-256 + Salt)...")
    pii_cols = pii_detector.detect_pii_fields("datasus_sih", raw_df)
    print(f"      🔒 Campos sensíveis detectados: {pii_cols}")

    anon_df, audit_log = anonymizer.anonymize(raw_df, pii_cols)
    print(f"      ✅ Pseudoanonimização concluída. Status: {audit_log['status']}")
    if "NASC" in anon_df.columns and not anon_df["NASC"].dropna().empty:
        sample_nasc = anon_df["NASC"].dropna().iloc[0]
        print(f"      Amos. NASC mascarado: {str(sample_nasc)[:20]}... (64 hex chars)")

    # Step 4: Data Quality Validation
    print("\n[4/6] Validando integridade e schema do SIH...")
    val_result = validator.validate(anon_df)
    print(f"      ✅ Registros válidos: {len(val_result.valid_df)} | Rejeitados: {len(val_result.rejected_df)}")

    # Step 5: Write to Lakehouse Bronze (Delta Lake)
    print("\n[5/6] Persistindo na Camada Bronze (Delta Lake)...")
    metadata = {
        "source": "datasus",
        "subsystem": "sih",
        "source_type": "datasus_sih",
        "source_file": os.path.basename(dbc_file),
    }
    write_stats = bronze_writer.write(val_result.valid_df, metadata)
    print(f"      ✅ Bronze Delta Table atualizada: {write_stats['table_path']}")
    print(f"      ⏱️ Duração da escrita: {write_stats['duration_seconds']}s")

    # Step 6: Transform to Silver (Normalização Semântica & MPI)
    print("\n[6/6] Executando pipeline de normalização semântica para Camada Silver...")
    silver_pipeline = SilverTransformationPipeline()
    silver_dataset = silver_pipeline.transform_dataframe(
        val_result.valid_df,
        source_type="datasus_sih",
        source_file=os.path.basename(dbc_file)
    )

    print("\n" + "=" * 70)
    print("  🎉 TESTE DATASUS CONCLUÍDO COM SUCESSO!")
    print("=" * 70)
    print(f"  📥 Internações brutas baixadas:  {len(raw_df)}")
    print(f"  🔒 LGPD Campos Mascarados:       {len(pii_cols)}")
    print(f"  💾 Tabela Bronze Delta:          {write_stats['table_path']}")
    print(f"  🏛️ Entidades Silver Geradas:")
    for entity, count in silver_dataset.summary().items():
        print(f"     - {entity.capitalize():<15}: {count} registros")
    print("=" * 70)


if __name__ == "__main__":
    main()
