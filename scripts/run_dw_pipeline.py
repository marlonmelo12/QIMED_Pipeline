"""
Script de Execução e Consulta do Data Warehouse do QIMED (DuckDB + Delta Lake).
Processa os Data Marts da Camada Gold e executa diagnósticos analíticos executivos.
"""
import os
import sys
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.gold.pipeline import GoldTransformationPipeline
from src.dw.dw_manager import DataWarehouseManager


def main():
    print("=" * 85)
    print("       QIMED MODERN DATA WAREHOUSE (DW) - CARGA E ANALYTICS")
    print("=" * 85)

    # 1. Executar o Pipeline da Camada Gold
    pipeline = GoldTransformationPipeline()
    stats = pipeline.run()

    print(f"\n✅ Carga do Data Warehouse finalizada em {stats['duration_seconds']} segundos.")
    print(f"📦 Arquivo do Data Warehouse: {stats['dw_path']}")
    print("\n--- Data Marts Gerados ---")
    for mart, count in stats["data_marts"].items():
        print(f"• {mart:<30}: {count:>6} registros agregados")

    # 2. Conectar ao DuckDB e Executar Queries Analíticas nas Views Semânticas
    print("\n" + "=" * 85)
    print("               DIAGNÓSTICOS EXECUTIVOS DO DATA WAREHOUSE")
    print("=" * 85)

    dw = DataWarehouseManager()
    
    # 2.1. Sumário Executivo
    print("\n📊 1. SUMÁRIO EXECUTIVO GLOBAL DA REDE (vw_kpi_sumario_executivo):")
    df_sum = dw.query_df("SELECT * FROM vw_kpi_sumario_executivo;")
    print(df_sum.T.rename(columns={0: "Valor"}).to_string())

    # 2.2. Top 5 Prestadores com Maiores Glosas Financeiras
    print("\n🚨 2. TOP 5 PRESTADORES COM MAIORES PERDAS POR GLOSA (vw_kpi_ranking_glosas_hospital):")
    df_glo = dw.query_df("""
        SELECT hospital_name, municipality_name, total_faturado_brl, total_aprovado_brl, total_glosado_brl, taxa_glosa_pct
        FROM vw_kpi_ranking_glosas_hospital
        WHERE total_glosado_brl > 0
        LIMIT 5;
    """)
    if not df_glo.empty:
        print(df_glo.to_string(index=False))
    else:
        print("Nenhuma glosa com valor maior que zero encontrada na amostra.")

    # 2.3. Top 5 Hospitais em Volume e Desempenho Clínico
    print("\n🏥 3. DESEMPENHO E EFICIÊNCIA HOSPITALAR (vw_kpi_eficiencia_hospitalar):")
    df_eff = dw.query_df("""
        SELECT hospital_name, total_internacoes, tempo_medio_permanencia_dias, custo_medio_internacao_brl, taxa_mortalidade_pct, leitos_totais_cadastrados
        FROM vw_kpi_eficiencia_hospitalar
        ORDER BY total_internacoes DESC
        LIMIT 5;
    """)
    print(df_eff.to_string(index=False))

    # 2.4. Top 5 Patologias com Maiores Readmissões em 30 Dias
    print("\n🔄 4. READMISSÕES PRECOCES EM 30 DIAS POR PATOLOGIA (vw_kpi_readmissoes_criticas):")
    df_readm = dw.query_df("""
        SELECT cid10_code, disease_name, total_pacientes, total_internacoes, readmissoes_30_dias, taxa_readmissao_30_dias_pct, custo_total_readmissoes_brl
        FROM vw_kpi_readmissoes_criticas
        WHERE readmissoes_30_dias > 0
        ORDER BY readmissoes_30_dias DESC
        LIMIT 5;
    """)
    if not df_readm.empty:
        print(df_readm.to_string(index=False))
    else:
        print("Sem readmissões identificadas no filtro.")

    # 2.5. Gargalos de Regulação e Filas do SISREG
    print("\n⏳ 5. GARGALOS DE REGULAÇÃO E FILAS DE LEITOS (vw_kpi_filas_regulacao_uti):")
    df_filas = dw.query_df("""
        SELECT municipality_name, referral_type, total_solicitacoes, solicitacoes_autorizadas, solicitacoes_pendentes_fila, taxa_autorizacao_pct, tempo_medio_espera_dias
        FROM vw_kpi_filas_regulacao_uti
        LIMIT 5;
    """)
    print(df_filas.to_string(index=False))

    dw.close()
    print("\n" + "=" * 85)
    print("✅ Execução do Data Warehouse concluída com sucesso!")
    print("=" * 85)


if __name__ == "__main__":
    main()
