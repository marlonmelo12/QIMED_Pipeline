"""
Script de Execucao Completa do Pipeline e DAGs do QIMED DataQore.
Executa sequencialmente o fluxo real de dados sem hardcode:
1. Ingestao e Download das Fontes (SIH, SIA, CNES, SISREG, ANS)
2. Persistencia Bronze (Delta Lake com auditoria e anonimizacao)
3. Transformacao Silver (Normalizacao, Resolucao de Entidades e Mapeamentos Semanticos)
4. Agregacao Gold (Data Marts Nacional, Estadual, Municipal e Views DuckDB)
"""
import os
import sys
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pipeline.master_pipeline import QimedMasterPipeline
from src.dw.dw_manager import DataWarehouseManager


def main():
    print("=" * 88)
    print("      QIMED HEALTH LAKEHOUSE & DW - EXECUCAO COMPLETA DO PIPELINE")
    print("=" * 88)
    print("Iniciando orquestracao ponta a ponta...")

    pipeline = QimedMasterPipeline()
    results = pipeline.run_full_pipeline()

    print("\n" + "=" * 88)
    print(f"[OK] Pipeline finalizado com sucesso em {results['duration_seconds']} segundos!")
    print("=" * 88)

    # Validacao e Exibicao dos Resultados no Data Warehouse
    dw = DataWarehouseManager()
    
    print("\n1. RESUMO EXECUTIVO NACIONAL (BRASIL - 27 UFs):")
    df_nac = dw.query_df("SELECT * FROM vw_kpi_nacional_sumario")
    print(df_nac.to_string(index=False))

    print("\n2. TOP 5 ESTADOS COM MAIOR OCUPACAO DE LEITOS:")
    df_est = dw.query_df("""
        SELECT 
            uf_sigla, estado_nome, regiao, total_internacoes, leitos_totais_cnes,
            taxa_ocupacao_leitos_pct, custo_total_brl, total_glosado_brl, taxa_glosa_pct
        FROM vw_kpi_estado_ocupacao_e_glosas
        ORDER BY taxa_ocupacao_leitos_pct DESC
        LIMIT 5;
    """)
    print(df_est.to_string(index=False))

    print("\n3. TOP 5 MUNICIPIOS COM MAIOR VOLUME DE INTERNACOES:")
    df_mun = dw.query_df("""
        SELECT 
            uf_sigla, municipality_code, municipality_name, total_internacoes,
            internacoes_icsap_evitaveis, taxa_icsap_pct, custo_total_brl
        FROM vw_kpi_municipio_saude_e_icsap
        ORDER BY total_internacoes DESC
        LIMIT 5;
    """)
    print(df_mun.to_string(index=False))

    dw.close()
    print("\n" + "=" * 88)
    print("[OK] Todas as etapas (Download -> Bronze -> Silver -> Gold) estao ativas e persistidas!")
    print("=" * 88)


if __name__ == "__main__":
    main()
