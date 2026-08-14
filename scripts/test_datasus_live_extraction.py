"""
Teste de Extracao Real e Pipeline Ponta a Ponta via FTP DATASUS (SIH e CNES).
Realiza download de arquivos DBC de ftp.datasus.gov.br, descompressao,
anonimizacao LGPD, escrita na Bronze e transformacao na Silver Delta Lake.
"""
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

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
    print("  QIMED DataQore — Teste de Extracao Real DATASUS (FTP -> Bronze -> Silver)")
    print("=" * 70)

    subsystem = "SIH"
    uf = "AC"
    year = 2026
    month = 1

    print(f"\n[1/6] Conectando ao FTP do DATASUS (ftp.datasus.gov.br)...")
    print(f"      Subsistema: {subsystem} | UF: {uf} | Competencia: {year}/{month:02d}")

    collector = DatasusCollector(subsystem=subsystem, uf=uf, year=year, month=month)
    pii_detector = PIIDetector()
    anonymizer = Anonymizer(salt="qimed_prod_salt_2026")
    validator = DatasusValidator(subsystem=subsystem)
    bronze_writer = BronzeWriter(lakehouse_path=os.path.join(PROJECT_ROOT, "lakehouse", "bronze"))

    # 1. Download DBC via FTP
    start_dl = time.time()
    dbc_file = collector.fetch()
    dl_time = round(time.time() - start_dl, 2)
    print(f"      Download concluido em {dl_time}s: {os.path.basename(dbc_file)}")

    # 2. Descompressao DBC -> DBF e Parsing
    print("\n[2/6] Descomprimindo formato proprietario DBC e extraindo DBF...")
    start_parse = time.time()
    raw_df = collector.parse(dbc_file)
    parse_time = round(time.time() - start_parse, 2)
    print(f"      Descompressao e parsing concluidos em {parse_time}s")
    print(f"      Total de internacoes extraidas: {len(raw_df)} registros")
    print(f"      Colunas disponiveis ({len(raw_df.columns)}): {list(raw_df.columns[:10])}...")

    # 3. Deteccao e Anonimizacao LGPD
    print("\n[3/6] Passando pelo LGPD Gate (Deteccao e Hashing SHA-256 + Salt)...")
    pii_cols = pii_detector.detect_pii_fields("datasus_sih", raw_df)
    print(f"      Campos sensiveis detectados: {pii_cols}")

    anon_df, audit_log = anonymizer.anonymize(raw_df, pii_cols)
    print(f"      Pseudoanonimizacao concluida. Status: {audit_log['status']}")

    # 4. Validacao de Qualidade dos Dados
    print("\n[4/6] Validando integridade e schema do SIH...")
    val_result = validator.validate(anon_df)
    print(f"      Registros validos: {len(val_result.valid_df)} | Rejeitados: {len(val_result.rejected_df)}")

    # 5. Gravacao na Camada Bronze (Delta Lake)
    print("\n[5/6] Persistindo na Camada Bronze (Delta Lake)...")
    metadata = {
        "source": "datasus",
        "subsystem": "sih",
        "source_type": "datasus_sih",
        "source_file": os.path.basename(dbc_file),
    }
    write_stats = bronze_writer.write(val_result.valid_df, metadata)
    print(f"      Bronze Delta Table atualizada: {write_stats['table_path']}")
    print(f"      Duracao da escrita: {write_stats['duration_seconds']}s")

    # 6. Transformacao para a Camada Silver
    print("\n[6/6] Executando pipeline de normalizacao semantica para Camada Silver...")
    silver_pipeline = SilverTransformationPipeline(
        bronze_base_path=os.path.join(PROJECT_ROOT, "lakehouse", "bronze"),
        silver_base_path=os.path.join(PROJECT_ROOT, "lakehouse", "silver")
    )
    silver_dataset = silver_pipeline.transform_dataframe(
        val_result.valid_df,
        source_type="datasus_sih",
        source_file=os.path.basename(dbc_file)
    )

    print("\n" + "=" * 70)
    print("  TESTE DATASUS CONCLUIDO COM SUCESSO")
    print("=" * 70)
    print(f"  Internacoes brutas baixadas:  {len(raw_df)}")
    print(f"  Campos PII mascarados:       {len(pii_cols)}")
    print(f"  Tabela Bronze Delta:         {write_stats['table_path']}")
    print(f"  Entidades Silver Geradas:")
    for entity, count in silver_dataset.summary().items():
        print(f"     - {entity.capitalize():<15}: {count} registros")
    print("=" * 70)


if __name__ == "__main__":
    main()
