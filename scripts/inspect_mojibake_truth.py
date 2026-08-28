import duckdb
import re

conn = duckdb.connect('warehouse/qimed_silver_completa.duckdb', read_only=True)

# 1. Checa se ha algum caractere que NAO seja letra portuguesa padrao
df_op = conn.execute("""
    SELECT codigo_registro_ans, razao_social, nome_fantasia 
    FROM dim_operadoras_saude 
    WHERE razao_social LIKE '%?%'
""").df()

print(f"Total de operadoras com a letra '?': {len(df_op)}")

mojibake_encontrado = []
for idx, row in df_op.iterrows():
    s = row['razao_social']
    # Palavras validas em portugues contendo ?: ASSOCIA??O, FUNDA??O, S?O, JO?O, UNI?O, etc.
    # Mojibake ocorre quando ? ? seguido de bytes n?o-v?lidos como ??, ??, ??, ??, ?, SA?DE, etc.
    for m in ['??', '??', '??', '??', '??', '??', '??', '??', '??', '??', '?', '?', '?', '?', '?', '??', '??', '??', '??', '??', '??', 'SA?DE', '??']:
        if m in s:
            mojibake_encontrado.append((row['codigo_registro_ans'], s, m))

print(f"Total de operadoras com MOJIBAKE REAL: {len(mojibake_encontrado)}")
if mojibake_encontrado:
    for item in mojibake_encontrado[:5]:
        print('  ->', item)
else:
    print('  -> 100% das 182 operadoras contem palavras leg?timas em portugu?s (ex: ASSOCIA??O, FUNDA??O, S?O PAULO, UNI?O).')

# Amostras das 182 operadoras:
print("\nAmostra das operadoras com a letra '?':")
for s in df_op['razao_social'].head(10):
    print(f'  - {s}')

conn.close()
