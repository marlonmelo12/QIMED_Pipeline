import os
import sys
import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.silver.ceara_mappings import resolver_nome_doenca_expandido

DW_PATH = "warehouse/qimed_dw.duckdb"

def analyze_ipu():
    conn = duckdb.connect(DW_PATH, read_only=True)
    
    print("=" * 85)
    print("🏥 DIAGNÓSTICO EXECUTIVO DE SAÚDE - IPU / CE (IBGE: 230580)")
    print("=" * 85)

    # 1. Panorama Geral de Pacientes e Internações
    df_geral = conn.execute("""
        SELECT 
            COUNT(DISTINCT p.patient_master_id) AS total_pacientes_ipu,
            COUNT(e.encounter_id) AS total_internacoes_aih,
            ROUND(SUM(e.total_cost_brl), 2) AS custo_total_internacoes_brl,
            ROUND(AVG(e.length_of_stay_days), 1) AS media_permanencia_dias,
            ROUND(SUM(CASE WHEN e.discharge_disposition = 'expired' THEN 1 ELSE 0 END) * 100.0 / COUNT(e.encounter_id), 2) AS taxa_mortalidade_pct
        FROM dim_patients p
        LEFT JOIN fct_encounters e ON p.patient_master_id = e.patient_master_id
        WHERE p.municipality_code = '230580' OR p.municipality_name = 'Ipu';
    """).df()
    print("\n📊 1. PANORAMA GERAL DOS PACIENTES RESIDENTES EM IPU:")
    print(df_geral.to_string(index=False))

    # 2. Top 10 Patologias que Mais Internam Moradores de Ipu (CID-10)
    df_cid = conn.execute("""
        SELECT 
            e.primary_diagnosis_code AS cid10,
            COUNT(*) AS total_internacoes,
            ROUND(SUM(e.total_cost_brl), 2) AS custo_total_brl,
            ROUND(AVG(e.length_of_stay_days), 1) AS media_dias_leito
        FROM dim_patients p
        JOIN fct_encounters e ON p.patient_master_id = e.patient_master_id
        WHERE p.municipality_code = '230580' OR p.municipality_name = 'Ipu'
        GROUP BY 1
        ORDER BY total_internacoes DESC
        LIMIT 10;
    """).df()
    df_cid["descricao_clinica"] = df_cid["cid10"].apply(resolver_nome_doenca_expandido)
    print("\n🩺 2. TOP 10 MOTIVOS DE INTERNAÇÃO DE MORADORES DE IPU (CID-10):")
    print(df_cid[["cid10", "descricao_clinica", "total_internacoes", "custo_total_brl", "media_dias_leito"]].to_string(index=False))

    # 3. Fluxo de Encaminhamento: Onde os Pacientes de Ipu são Internados?
    df_hosp = conn.execute("""
        SELECT 
            e.hospital_name,
            COUNT(*) AS total_internacoes,
            ROUND(SUM(e.total_cost_brl), 2) AS valor_pago_sus_brl,
            ROUND(AVG(e.length_of_stay_days), 1) AS media_dias
        FROM dim_patients p
        JOIN fct_encounters e ON p.patient_master_id = e.patient_master_id
        WHERE p.municipality_code = '230580' OR p.municipality_name = 'Ipu'
        GROUP BY 1
        ORDER BY total_internacoes DESC
        LIMIT 8;
    """).df()
    print("\n🏥 3. REDE DE HOSPITAIS QUE ATENDEU OS PACIENTES DE IPU:")
    print(df_hosp.to_string(index=False))

    # 4. Indicador de Prevenção e Internações Evitáveis (ICSAP) em Ipu
    df_icsap = conn.execute("""
        SELECT *
        FROM dm_icsap_prevention
        WHERE municipality_name = 'Ipu' OR municipality_name LIKE '%Ipu%';
    """).df()
    print("\n📉 4. INDICADORES DE ATENÇÃO BÁSICA E INTERNAÇÕES EVITÁVEIS (ICSAP) EM IPU:")
    print(df_icsap.to_string(index=False))

    # 5. Glosas Ambulatoriais e Procedimentos do Município (SIA)
    df_glo = conn.execute("""
        SELECT *
        FROM dm_glosas_auditoria
        WHERE municipality_name = 'Ipu' OR municipality_code = '230580';
    """).df()
    print("\n🚨 5. AUDITORIA DE GLOSAS EM PRESTADORES DE IPU:")
    if not df_glo.empty:
        print(df_glo.to_string(index=False))
    else:
        print("Nenhum faturamento com glosa registrado para os prestadores cadastrados em Ipu.")

    conn.close()

if __name__ == "__main__":
    analyze_ipu()
