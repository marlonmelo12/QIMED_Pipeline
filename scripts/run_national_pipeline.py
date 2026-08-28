"""
Script de Execucao do Pipeline de Engenharia de Dados Nacional QIMED.
Executa o processamento completo das camadas Bronze, Prata e Ouro do Lakehouse e Data Warehouse.
Disponibiliza os KPIs em tres visoes hierarquicas: Brasil (Nacional), Estados (27 UFs) e Municipios.
"""
import os
import sys
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.gold.pipeline_nacional import UnifiedNationalDataPipeline
from src.dw.dw_manager import DataWarehouseManager


def main():
    print("=" * 88)
    print("      QIMED HEALTH LAKEHOUSE & DW - PIPELINE NACIONAL (BRONZE / PRATA / OURO)")
    print("=" * 88)

    # 1. Calculo de Memoria e Estimativa de Volume
    print("\n1. DIMENSIONAMENTO PREVIO DE MEMORIA (RAM FOOTPRINT):")
    print("- Escopo: 27 Unidades da Federacao (26 Estados + Distrito Federal)")
    print("- Periodo: Mes de Julho (31 dias de producao hospitalar e ambulatorial)")
    print("- Camada Bronze (Delta Lake): lakehouse/bronze/datasus/ (sih, sia, cnes)")
    print("- Camada Prata (Delta Lake):  lakehouse/silver/ (dim_patients, fct_encounters, fct_conditions)")
    print("- Camada Ouro (Data Marts):   lakehouse/gold/ (dm_nacional_kpis, dm_estado_kpis, dm_municipio_kpis)")
    print("- Data Warehouse:             warehouse/qimed_dw.duckdb")
    print("- Consumo Estimado de RAM:    1.8 GB a 2.2 GB RAM (Limite seguro: 3.0 GB no DuckDB)")
    print("- Armazenamento em Disco:     ~550 MB compactado em Parquet/Snappy")

    # 2. Executar Pipeline
    print("\nExecutando pipeline unificado...")
    pipeline = UnifiedNationalDataPipeline()
    stats = pipeline.run()

    print(f"\n[OK] Pipeline concluido com sucesso em {stats['duration_seconds']} segundos.")
    print(f"[OK] Total de Estados Processados:    {stats['estados_processados']} UFs")
    print(f"[OK] Total de Municipios Mapeados:   {stats['municipios_processados']} cidades")
    print(f"[OK] Internacoes Hospitalares:       {stats['total_internacoes_brasil']:,}")
    print(f"[OK] Perdas por Glosas Rastreadas:   R$ {stats['total_glosado_brasil_brl']:,.2f}")

    # 3. Consultas Analiticas no Data Warehouse
    dw = DataWarehouseManager()

    print("\n" + "=" * 88)
    print("         1. VISAO NACIONAL (BRASIL - JULHO)")
    print("=" * 88)
    df_nac = dw.query_df("SELECT * FROM vw_kpi_nacional_sumario")
    print(df_nac.to_string(index=False))

    print("\n" + "=" * 88)
    print("         2. VISAO ESTADUAL (TOP 10 ESTADOS POR OCUPACAO E GLOSAS)")
    print("=" * 88)
    df_est = dw.query_df("""
        SELECT 
            uf_sigla,
            estado_nome,
            regiao,
            total_internacoes,
            leitos_totais_cnes,
            taxa_ocupacao_leitos_pct,
            custo_total_brl,
            total_glosado_brl,
            taxa_glosa_pct
        FROM vw_kpi_estado_ocupacao_e_glosas
        ORDER BY taxa_ocupacao_leitos_pct DESC
        LIMIT 10;
    """)
    print(df_est.to_string(index=False))

    print("\n" + "=" * 88)
    print("         3. VISAO MUNICIPAL (DETALHE POR MUNICIPIO & ICSAP)")
    print("=" * 88)
    df_mun = dw.query_df("""
        SELECT 
            uf_sigla,
            municipality_code,
            municipality_name,
            total_internacoes,
            internacoes_icsap_evitaveis,
            taxa_icsap_pct,
            custo_total_brl,
            tempo_medio_permanencia_dias
        FROM vw_kpi_municipio_saude_e_icsap
        ORDER BY total_internacoes DESC
        LIMIT 12;
    """)
    print(df_mun.to_string(index=False))

    dw.close()
    print("\n" + "=" * 88)
    print("[OK] Execucao finalizada com sucesso.")
    print("=" * 88)


if __name__ == "__main__":
    main()
