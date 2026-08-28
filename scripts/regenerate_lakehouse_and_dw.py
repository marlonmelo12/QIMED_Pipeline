"""
Script Mestre de Regeneração e Atualização do QIMED Lakehouse & DW V3.
Executa a atualização das camadas Bronze, Silver e Gold em conformidade com todas as regras das Tasks 1 a 19.
"""
import os
import sys
import time
import duckdb
import pandas as pd
import pyarrow as pa
from deltalake.writer import write_deltalake

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.processing.duckdb_engine import DuckDBEngine
from src.processing.transformations import CanonicalTransformations
from src.gold.pipeline_nacional import GoldPipelineNacional
from src.collectors.ans_collector import AnsCollector
from src.gold.models.kpi_central_anomalias import build_aud_alertas_anomalias
from src.gold.models.views_semanticas import registrar_views_semanticas
from src.dw.maintenance import otimizar_storage_duckdb


def pfix(p: str) -> str:
    return p.replace(chr(92), "/")


def main():
    print("=" * 88)
    print("      REGENERAÇÃO E ATUALIZAÇÃO DO LAKEHOUSE E DATA WAREHOUSE QIMED V3")
    print("=" * 88)
    t0_global = time.time()
    execution_id = f"exec_regen_{int(time.time())}"

    # -------------------------------------------------------------
    # ETAPA 0: EXCLUSÃO DO BANCO SILVER DUCKDB
    # -------------------------------------------------------------
    silver_db_path = os.path.join(PROJECT_ROOT, "warehouse", "qimed_silver_completa.duckdb")
    if os.path.exists(silver_db_path):
        print(f"\n🗑️ [Etapa 0/7] Excluindo banco físico anterior: {silver_db_path}...")
        try:
            os.remove(silver_db_path)
            print("  ✓ Arquivo qimed_silver_completa.duckdb removido com sucesso.")
        except Exception as e:
            print(f"  ⚠️ Aviso ao remover arquivo ({e}). Será recriado.")

    engine = DuckDBEngine()
    transforms = CanonicalTransformations(duck_engine=engine)

    # -------------------------------------------------------------
    # ETAPA 1: DIMENSÃO TEMPO (Task 1)
    # -------------------------------------------------------------
    print("\n📅 [Etapa 1/7] Regenerando dim_tempo (1.095 registros, 2025-2027, mode=overwrite)...")
    t0 = time.time()
    transforms.gerar_dim_tempo(start_year=2025, end_year=2027, execution_id=execution_id)
    print(f"  ✓ dim_tempo concluída em {time.time() - t0:.2f}s")

    # -------------------------------------------------------------
    # ETAPA 2: SIH SILVER E MPI (Tasks 5, 6, 7, 11, 16)
    # -------------------------------------------------------------
    print("\n🏥 [Etapa 2/7] Regenerando fct_internacao e dim_paciente com PK, Episódios, MPI Neonatal e CIDs higienizados...")
    t0 = time.time()
    transforms.transformar_sih_para_silver(execution_id=execution_id)
    print(f"  ✓ fct_internacao e dim_paciente concluídas em {time.time() - t0:.2f}s")

    # -------------------------------------------------------------
    # ETAPA 3: ANS SILVER - OPERADORAS E RESSARCIMENTO (Tasks 4, 8, 14, 15, 17)
    # -------------------------------------------------------------
    print("\n📦 [Etapa 3/7] Atualizando dados da ANS (dim_operadoras_saude e fct_ressarcimento_sus com Bridge MPI e Trava Contábil)...")
    t0 = time.time()
    transforms.transformar_ans_para_silver(execution_id=execution_id)
    print(f"  ✓ ANS Silver concluída em {time.time() - t0:.2f}s")

    # -------------------------------------------------------------
    # ETAPA 4: SIA SILVER (Tasks 2, 3, 6, 11, 12)
    # -------------------------------------------------------------
    print("\n🩺 [Etapa 4/7] Regenerando fct_atendimentos_ambulatoriais com PK e Sentinelas higienizados...")
    t0 = time.time()
    transforms.transformar_sia_para_silver(execution_id=execution_id)
    print(f"  ✓ fct_atendimentos_ambulatoriais concluída em {time.time() - t0:.2f}s")

    # -------------------------------------------------------------
    # ETAPA 5: GLOSAS HOSPITALARES (Tasks 9 e 19)
    # -------------------------------------------------------------
    print("\n📋 [Etapa 5/7] Gerando fct_glosas_hospitalares (SIH-RJ + SIH-ER)...")
    t0 = time.time()
    transforms.transformar_glosas_hospitalares_para_silver(execution_id=execution_id)
    print(f"  ✓ fct_glosas_hospitalares concluída em {time.time() - t0:.2f}s")

    # -------------------------------------------------------------
    # ETAPA 6: CAMADA GOLD & CENTRAL DE ANOMALIAS (Tasks 10, 13, 18)
    # -------------------------------------------------------------
    print("\n📊 [Etapa 6/7] Materializando Data Marts Gold e Central de Anomalias (aud_alertas_anomalias)...")
    t0 = time.time()
    gold_pipeline = GoldPipelineNacional()
    gold_res = gold_pipeline.build_gold_data_marts(execution_id=execution_id)
    print(f"  ✓ Camada Gold materializada em {time.time() - t0:.2f}s: {gold_res.get('tables_created', [])}")

    # -------------------------------------------------------------
    # ETAPA 7: SINCRONIZAÇÃO DUCKDB DW (warehouse/qimed_dw.duckdb e qimed_silver_completa.duckdb)
    # -------------------------------------------------------------
    print("\n💾 [Etapa 7/7] Sincronizando Data Warehouses DuckDB...")
    t0 = time.time()
    
    # 7.1 Sincroniza dim_tempo no DW
    dw_path = os.path.join(PROJECT_ROOT, "warehouse", "qimed_dw.duckdb")
    con_dw = duckdb.connect(dw_path)
    dt_silver = pfix(os.path.join(PROJECT_ROOT, "lakehouse", "silver", "dim_tempo"))
    if os.path.exists(dt_silver):
        con_dw.execute(f"CREATE OR REPLACE TABLE dim_tempo AS SELECT * FROM delta_scan('{dt_silver}')")
        print("  ✓ dim_tempo sincronizada em qimed_dw.duckdb (1.095 registros).")
    con_dw.close()

    # 7.2 Sincroniza qimed_silver_completa.duckdb
    con_sil = duckdb.connect(silver_db_path)
    tables_to_sync = [
        "dim_tempo",
        "dim_operadoras_saude",
        "dim_paciente",
        "fct_internacao",
        "fct_atendimentos_ambulatoriais",
        "fct_ressarcimento_sus",
        "fct_glosas_hospitalares"
    ]
    for tbl in tables_to_sync:
        t_dir = pfix(os.path.join(PROJECT_ROOT, "lakehouse", "silver", tbl))
        if os.path.exists(t_dir):
            con_sil.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM delta_scan('{t_dir}')")
            cnt = con_sil.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  ✓ {tbl}: {cnt:,} linhas sincronizadas em qimed_silver_completa.duckdb.")

    # 7.3 Materializa aud_alertas_anomalias em qimed_silver_completa.duckdb
    print("  ✓ Materializando aud_alertas_anomalias em qimed_silver_completa.duckdb...")
    build_aud_alertas_anomalias(con_sil, fct_internacao_source="fct_internacao", target_table="aud_alertas_anomalias")
    cnt_anomalias = con_sil.execute("SELECT COUNT(*) FROM aud_alertas_anomalias").fetchone()[0]
    print(f"  ✓ aud_alertas_anomalias: {cnt_anomalias:,} alertas registrados em qimed_silver_completa.duckdb.")

    # 7.4 Registra Views Semânticas Analíticas e Preditivas
    print("  ✓ Registrando views semânticas (vw_internacoes_consolidadas, vw_ml_treinamento_admissao)...")
    registrar_views_semanticas(con_sil)
    con_sil.close()

    # 7.5 Otimização e Compactação de Storage
    print("  ✓ Otimizando storage DuckDB (CHECKPOINT e VACUUM)...")
    otimizar_storage_duckdb(silver_db_path)
    otimizar_storage_duckdb(dw_path)

    print(f"  ✓ Sincronização DuckDB concluída em {time.time() - t0:.2f}s")

    dur_global = time.time() - t0_global
    print("\n" + "=" * 88)
    print(f"🎉 REGENERAÇÃO COMPLETA DO LAKEHOUSE E DW CONCLUÍDA EM {dur_global:.2f}s!")
    print("=" * 88)


if __name__ == "__main__":
    main()
