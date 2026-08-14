"""
QIMED DataQore — Runner para o Pipeline de Transformacao Bronze -> Silver.
Le as tabelas da camada Bronze Delta Lake -> Normalizacao Semantica -> Resolucao de Entidades -> Tabelas Silver Delta Lake.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.silver.pipeline import SilverTransformationPipeline
from src.utils.logging_config import get_logger

logger = get_logger("run_silver_pipeline")


def main():
    print("=" * 65)
    print("  QIMED DataQore — Pipeline de Transformacao (Bronze -> Silver)")
    print("=" * 65)

    bronze_path = os.path.join(PROJECT_ROOT, "lakehouse", "bronze")
    silver_path = os.path.join(PROJECT_ROOT, "lakehouse", "silver")

    pipeline = SilverTransformationPipeline(
        bronze_base_path=bronze_path,
        silver_base_path=silver_path
    )

    # 1. Transformar tabela Bronze FHIR Synthetic
    print("\n[Etapa 1/3] Processando Tabela Bronze FHIR Sintetica...")
    try:
        fhir_dataset = pipeline.transform_bronze_table("fhir/synthetic", source_type="fhir_synthetic")
        print("  Sucesso na transformacao FHIR:")
        for entity, count in fhir_dataset.summary().items():
            print(f"     - {entity.capitalize()}: {count} registros")
    except Exception as e:
        print(f"  Erro ao processar FHIR: {e}")

    # 2. Transformar tabela Bronze SIH se disponivel
    sih_bronze_path = os.path.join(bronze_path, "datasus", "sih")
    print("\n[Etapa 2/3] Verificando Tabela Bronze DATASUS SIH...")
    if os.path.exists(sih_bronze_path):
        try:
            sih_dataset = pipeline.transform_bronze_table("datasus/sih", source_type="datasus_sih")
            print("  Sucesso na transformacao SIH:")
            for entity, count in sih_dataset.summary().items():
                print(f"     - {entity.capitalize()}: {count} registros")
        except Exception as e:
            print(f"  Erro ao processar SIH: {e}")
    else:
        print("  Nenhuma tabela Bronze SIH encontrada.")

    # 3. Transformar tabela Bronze CNES se disponivel
    cnes_bronze_path = os.path.join(bronze_path, "datasus", "cnes")
    print("\n[Etapa 3/3] Verificando Tabela Bronze DATASUS CNES...")
    if os.path.exists(cnes_bronze_path):
        try:
            cnes_dataset = pipeline.transform_bronze_table("datasus/cnes", source_type="datasus_cnes")
            print("  Sucesso na transformacao CNES:")
            for entity, count in cnes_dataset.summary().items():
                print(f"     - {entity.capitalize()}: {count} registros")
        except Exception as e:
            print(f"  Erro ao processar CNES: {e}")
    else:
        print("  Nenhuma tabela Bronze CNES encontrada.")

    print("\n" + "=" * 65)
    print("  Transformacao da Camada Silver Concluida com Sucesso!")
    print(f"  Diretorio de Destino: {silver_path}")
    print("=" * 65)


if __name__ == "__main__":
    main()
