"""
QIMED DataQore — Script de ingestao FHIR sintetica para popular a camada Bronze.
Executa o pipeline completo: geracao -> parsing -> deteccao de PII -> anonimizacao -> validacao -> escrita Bronze.
"""
import os
import sys
import hashlib

# Adiciona a raiz do projeto ao sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.collectors.fhir_collector import FhirSyntheticCollector
from src.lgpd.pii_detector import PIIDetector
from src.lgpd.anonymizer import Anonymizer
from src.validators.fhir_validator import FhirValidator
from src.lakehouse.bronze_writer import BronzeWriter
from src.metadata.catalog import DatasetCatalog
from src.utils.logging_config import get_logger

logger = get_logger("run_fhir_ingestion")


def main():
    lakehouse_path = os.path.join(PROJECT_ROOT, "lakehouse", "bronze")
    os.makedirs(lakehouse_path, exist_ok=True)

    num_patients = 50
    encounters_per_patient = 3

    logger.info(f"Iniciando ingestao FHIR sintetica: {num_patients} pacientes, {encounters_per_patient} atendimentos cada")

    collector = FhirSyntheticCollector(
        num_patients=num_patients,
        encounters_per_patient=encounters_per_patient,
    )
    pii_detector = PIIDetector()
    anonymizer = Anonymizer(salt="qimed_dev_salt_2026")
    validator = FhirValidator()
    writer = BronzeWriter(lakehouse_path=lakehouse_path)
    catalog = DatasetCatalog()

    # 1. Geracao de Bundles FHIR sinteticos
    logger.info("Etapa 1: Gerando Bundles FHIR...")
    bundles = collector.fetch()
    logger.info(f"Bundles gerados: {len(bundles)}")

    # 2. Parsing tabular dos recursos
    logger.info("Etapa 2: Realizando parsing dos recursos...")
    df = collector.parse(bundles)
    logger.info(f"Recursos extraidos: {len(df)}")

    # 3. Deteccao de PII
    logger.info("Etapa 3: Identificando campos de dados pessoais (PII)...")
    pii_fields = pii_detector.detect_pii_fields("fhir_synthetic", df)
    logger.info(f"Campos PII detectados: {pii_fields}")

    # 4. Anonimizacao LGPD
    logger.info("Etapa 4: Aplicando hash criptografico SHA-256 + salt...")
    df_anon, audit_log = anonymizer.anonymize(df, pii_fields)

    # 5. Validacao de Schema e Integridade
    logger.info("Etapa 5: Validando recursos FHIR...")
    result = validator.validate(df_anon)

    # 6. Escrita na Camada Bronze (Delta Lake)
    logger.info("Etapa 6: Persistindo na camada Bronze (Delta Lake)...")
    metadata = {
        "source": "fhir",
        "subsystem": "synthetic",
        "source_type": "fhir_synthetic",
        "source_file": "in_memory_generation",
    }
    write_stats = writer.write(result.valid_df, metadata)

    # 7. Registro no Catalogo de Metadados
    logger.info("Etapa 7: Registrando dataset no catalogo...")
    schema_fp = hashlib.md5(str(list(result.valid_df.columns)).encode()).hexdigest()
    dataset_id = catalog.register_dataset(
        source_type="fhir_synthetic",
        partition_path=write_stats["table_path"],
        row_count=write_stats["rows_written"],
        schema_fingerprint=schema_fp,
        pii_anonymized=True,
    )

    print("\n" + "=" * 65)
    print("  QIMED DataQore — Ingestao FHIR Sintetica Concluida")
    print("=" * 65)
    print(f"  Pacientes gerados:     {num_patients}")
    print(f"  Total de recursos:     {len(df)}")
    print(f"  Campos PII mascarados: {len(pii_fields)}")
    print(f"  Linhas gravadas Bronze:{write_stats['rows_written']}")
    print(f"  Tabela Delta Lake:     {write_stats['table_path']}")
    print(f"  ID do Dataset:         {dataset_id}")
    print("=" * 65)


if __name__ == "__main__":
    main()
