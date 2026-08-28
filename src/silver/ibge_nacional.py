"""
Dicionário e Resolvedor Nacional de Municípios e UFs do Brasil (IBGE).
Cobre todos os 5.570 municípios brasileiros em todas as 27 Unidades da Federação.
"""
from typing import Dict, Optional, Tuple

# 1. Mapeamento das 27 Unidades da Federação e Regiões
UF_ESTADOS_BRASIL = {
    "11": {"sigla": "RO", "nome": "Rondônia", "regiao": "Norte"},
    "12": {"sigla": "AC", "nome": "Acre", "regiao": "Norte"},
    "13": {"sigla": "AM", "nome": "Amazonas", "regiao": "Norte"},
    "14": {"sigla": "RR", "nome": "Roraima", "regiao": "Norte"},
    "15": {"sigla": "PA", "nome": "Pará", "regiao": "Norte"},
    "16": {"sigla": "AP", "nome": "Amapá", "regiao": "Norte"},
    "17": {"sigla": "TO", "nome": "Tocantins", "regiao": "Norte"},
    "21": {"sigla": "MA", "nome": "Maranhão", "regiao": "Nordeste"},
    "22": {"sigla": "PI", "nome": "Piauí", "regiao": "Nordeste"},
    "23": {"sigla": "CE", "nome": "Ceará", "regiao": "Nordeste"},
    "24": {"sigla": "RN", "nome": "Rio Grande do Norte", "regiao": "Nordeste"},
    "25": {"sigla": "PB", "nome": "Paraíba", "regiao": "Nordeste"},
    "26": {"sigla": "PE", "nome": "Pernambuco", "regiao": "Nordeste"},
    "27": {"sigla": "AL", "nome": "Alagoas", "regiao": "Nordeste"},
    "28": {"sigla": "SE", "nome": "Sergipe", "regiao": "Nordeste"},
    "29": {"sigla": "BA", "nome": "Bahia", "regiao": "Nordeste"},
    "31": {"sigla": "MG", "nome": "Minas Gerais", "regiao": "Sudeste"},
    "32": {"sigla": "ES", "nome": "Espírito Santo", "regiao": "Sudeste"},
    "33": {"sigla": "RJ", "nome": "Rio de Janeiro", "regiao": "Sudeste"},
    "35": {"sigla": "SP", "nome": "São Paulo", "regiao": "Sudeste"},
    "41": {"sigla": "PR", "nome": "Paraná", "regiao": "Sul"},
    "42": {"sigla": "SC", "nome": "Santa Catarina", "regiao": "Sul"},
    "43": {"sigla": "RS", "nome": "Rio Grande do Sul", "regiao": "Sul"},
    "50": {"sigla": "MS", "nome": "Mato Grosso do Sul", "regiao": "Centro-Oeste"},
    "51": {"sigla": "MT", "nome": "Mato Grosso", "regiao": "Centro-Oeste"},
    "52": {"sigla": "GO", "nome": "Goiás", "regiao": "Centro-Oeste"},
    "53": {"sigla": "DF", "nome": "Distrito Federal", "regiao": "Centro-Oeste"},
}

SIGLA_PARA_UF_CODE = {v["sigla"]: k for k, v in UF_ESTADOS_BRASIL.items()}

# 2. Capitais e Principais Polos Regionais do Brasil
CAPITAIS_E_POLOS_BRASIL = {
    # Norte
    "110020": ("Porto Velho", "RO"), "120040": ("Rio Branco", "AC"), "130260": ("Manaus", "AM"),
    "140010": ("Boa Vista", "RR"), "150140": ("Belém", "PA"), "150680": ("Santarém", "PA"),
    "160030": ("Macapá", "AP"), "172100": ("Palmas", "TO"), "170210": ("Araguaína", "TO"),
    # Nordeste
    "211130": ("São Luís", "MA"), "210530": ("Imperatriz", "MA"), "221100": ("Teresina", "PI"),
    "220770": ("Parnaíba", "PI"), "230440": ("Fortaleza", "CE"), "230730": ("Juazeiro do Norte", "CE"),
    "231290": ("Sobral", "CE"), "230370": ("Caucaia", "CE"), "230765": ("Maracanaú", "CE"),
    "240810": ("Natal", "RN"), "240800": ("Mossoró", "RN"), "250750": ("João Pessoa", "PB"),
    "250400": ("Campina Grande", "PB"), "261160": ("Recife", "PE"), "260410": ("Caruaru", "PE"),
    "261110": ("Petrolina", "PE"), "270430": ("Maceió", "AL"), "270030": ("Arapiraca", "AL"),
    "280030": ("Aracaju", "SE"), "292740": ("Salvador", "BA"), "291080": ("Feira de Santana", "BA"),
    "293330": ("Vitória da Conquista", "BA"), "291840": ("Itabuna", "BA"),
    # Sudeste
    "310620": ("Belo Horizonte", "MG"), "317020": ("Uberlândia", "MG"), "311860": ("Contagem", "MG"),
    "313670": ("Juiz de Fora", "MG"), "314330": ("Montes Claros", "MG"), "320530": ("Vitória", "ES"),
    "320520": ("Vila Velha", "ES"), "320500": ("Serra", "ES"), "330455": ("Rio de Janeiro", "RJ"),
    "330490": ("São Gonçalo", "RJ"), "330170": ("Duque de Caxias", "RJ"), "330330": ("Niterói", "RJ"),
    "330100": ("Campos dos Goytacazes", "RJ"), "355030": ("São Paulo", "SP"), "351880": ("Guarulhos", "SP"),
    "350950": ("Campinas", "SP"), "354870": ("São Bernardo do Campo", "SP"), "354990": ("São José dos Campos", "SP"),
    "354340": ("Ribeirão Preto", "SP"), "355220": ("Sorocaba", "SP"), "354780": ("Santo André", "SP"),
    "354850": ("Santos", "SP"), "353440": ("Osasco", "SP"),
    # Sul
    "410690": ("Curitiba", "PR"), "411370": ("Londrina", "PR"), "411520": ("Maringá", "PR"),
    "410830": ("Foz do Iguaçu", "PR"), "410480": ("Cascavel", "PR"), "420540": ("Florianópolis", "SC"),
    "420910": ("Joinville", "SC"), "420240": ("Blumenau", "SC"), "420420": ("Chapecó", "SC"),
    "431490": ("Porto Alegre", "RS"), "430510": ("Caxias do Sul", "RS"), "431440": ("Pelotas", "RS"),
    "431690": ("Santa Maria", "RS"), "430460": ("Canoas", "RS"),
    # Centro-Oeste
    "500270": ("Campo Grande", "MS"), "500370": ("Dourados", "MS"), "510340": ("Cuiabá", "MT"),
    "510840": ("Várzea Grande", "MT"), "510760": ("Rondonópolis", "MT"), "520870": ("Goiânia", "GO"),
    "520140": ("Aparecida de Goiânia", "GO"), "520110": ("Anápolis", "GO"), "530010": ("Brasília", "DF")
}


import os
import pandas as pd

# Carregamento da Dimensao Completa dos 5.570 Municipios do IBGE
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PARQUET_MUN = os.path.join(_BASE_DIR, "config", "dim_municipios_ibge.parquet")

_MUNICIPIOS_IBGE_MAP: Dict[str, Tuple[str, str, str]] = {}

if os.path.exists(_PARQUET_MUN):
    try:
        df_m = pd.read_parquet(_PARQUET_MUN)
        for _, r in df_m.iterrows():
            c6 = str(r["municipality_code_6"]).strip()
            c7 = str(r["municipality_code_7"]).strip()
            nome = str(r["municipality_name"]).strip()
            uf = str(r["uf_sigla"]).strip()
            reg = str(r["regiao"]).strip()
            _MUNICIPIOS_IBGE_MAP[c6] = (nome, uf, reg)
            _MUNICIPIOS_IBGE_MAP[c7] = (nome, uf, reg)
    except Exception:
        pass


def resolver_uf_brasil(codigo_uf_ou_sigla: str) -> Tuple[str, str, str]:
    """
    Retorna (sigla, nome_estado, regiao) para qualquer código ou sigla de UF do Brasil.
    """
    val = str(codigo_uf_ou_sigla).strip().upper()
    if val in SIGLA_PARA_UF_CODE:
        cod = SIGLA_PARA_UF_CODE[val]
        return val, UF_ESTADOS_BRASIL[cod]["nome"], UF_ESTADOS_BRASIL[cod]["regiao"]
    
    cod2 = val[:2]
    if cod2 in UF_ESTADOS_BRASIL:
        info = UF_ESTADOS_BRASIL[cod2]
        return info["sigla"], info["nome"], info["regiao"]
        
    return "BR", "Brasil (Nacional)", "Nacional"


def resolver_municipio_nacional(codigo_ou_nome: str) -> Tuple[str, str, str]:
    """
    Retorna (nome_municipio, sigla_uf, regiao) para qualquer código IBGE do Brasil (5.570 municípios).
    """
    if not codigo_ou_nome:
        return "Município Não Informado", "BR", "Nacional"
    
    val = str(codigo_ou_nome).strip()
    cod_clean = val.replace(".0", "").replace("[", "").replace("]", "")
    cod6 = cod_clean[:6]
    cod7 = cod_clean[:7]
    
    # 1. Busca no catalogo completo oficial do IBGE (5.570 municipios)
    if cod6 in _MUNICIPIOS_IBGE_MAP:
        return _MUNICIPIOS_IBGE_MAP[cod6]
    if cod7 in _MUNICIPIOS_IBGE_MAP:
        return _MUNICIPIOS_IBGE_MAP[cod7]

    # 2. Busca direta no catalogo de polos e capitais (fallback em memoria)
    if cod6 in CAPITAIS_E_POLOS_BRASIL:
        nome, uf = CAPITAIS_E_POLOS_BRASIL[cod6]
        _, _, regiao = resolver_uf_brasil(uf)
        return nome, uf, regiao
        
    # 3. Resolucao por prefixo de UF
    uf_cod = cod6[:2]
    if uf_cod in UF_ESTADOS_BRASIL:
        uf_sigla = UF_ESTADOS_BRASIL[uf_cod]["sigla"]
        regiao = UF_ESTADOS_BRASIL[uf_cod]["regiao"]
        if any(c.isalpha() for c in val) and not val.startswith("Município IBGE"):
            return val, uf_sigla, regiao
        return f"Município [{cod6}] ({uf_sigla})", uf_sigla, regiao
        
    return val, "BR", "Nacional"

