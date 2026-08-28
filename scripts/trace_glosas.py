import sqlite3
import pandas as pd
import json

conn = sqlite3.connect('exports/qimed_health_lakehouse.db')

query = """
SELECT 
    PA_CODUNI,
    hospital_name,
    PA_UFMUN,
    municipality_name,
    PA_PROC_ID,
    PA_QTDPRO,
    PA_QTDAPR,
    PA_VALPRO,
    PA_VALAPR,
    (CAST(PA_VALPRO AS FLOAT) - CAST(PA_VALAPR AS FLOAT)) AS valor_glosado,
    PA_CATEND,
    PA_SEXO,
    PA_IDADE,
    PA_CIDPRI,
    PA_MOTSAI,
    PA_FLER
FROM sia_ambulatorial
WHERE CAST(PA_VALPRO AS FLOAT) > CAST(PA_VALAPR AS FLOAT)
  AND CAST(PA_VALPRO AS FLOAT) > 0
LIMIT 5;
"""

df = pd.read_sql(query, conn)

# Dicionario SIGTAP basico para procedimentos encontrados
sigtap_dict = {
    "0205020143": "ULTRASSONOGRAFIA DOPPLER DE FLUXO OBSTETRICO",
    "0301010048": "CONSULTA DE PROFISSIONAIS DE NIVEL SUPERIOR NA ATENCAO ESPECIALIZADA (EXCETO MEDICO)",
    "0301060061": "ATENDIMENTO DE URGENCIA COM OBSERVACAO ATE 24 HORAS EM ATENCAO ESPECIALIZADA",
    "0301060029": "ATENDIMENTO MEDICO EM UNIDADE DE PRONTO ATENDIMENTO (UPA)",
}

for i, r in df.iterrows():
    proc_name = sigtap_dict.get(str(r["PA_PROC_ID"]), f"Procedimento SIGTAP {r['PA_PROC_ID']}")
    print(f"=== GLOSA RASTREADA #{i+1} ===")
    print(f"• Estabelecimento: CNES {r['PA_CODUNI']} - {r['hospital_name']}")
    print(f"• Município: {r['municipality_name']} (IBGE: {r['PA_UFMUN']})")
    print(f"• Procedimento Faturado: {r['PA_PROC_ID']} - {proc_name}")
    print(f"• Produção Reivindicada: {r['PA_QTDPRO']} procedimento(s) | Valor Cobrado: R$ {float(r['PA_VALPRO']):.2f}")
    print(f"• Aprovação do SUS:      {r['PA_QTDAPR']} procedimento(s) | Valor Pago:    R$ {float(r['PA_VALAPR']):.2f}")
    print(f"• Prejuízo / Valor Glosado: R$ {float(r['valor_glosado']):.2f} (100% GLOSADO)")
    print(f"• Perfil Demográfico: Sexo {r['PA_SEXO']}, Idade {r['PA_IDADE']} anos, Caráter Atendimento: {r['PA_CATEND']}")
    print(f"• CID-10 Principal: {r['PA_CIDPRI']}")
    print("-" * 75)

conn.close()
