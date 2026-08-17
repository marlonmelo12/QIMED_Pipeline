"""
Mapeamento Completo de Todos os 22 Municipios do Estado do Acre (IBGE).
Inclui codigo de 6 digitos (DATASUS) e de 7 digitos (IBGE padrao).
"""

IBGE_MUNICIPIOS_ACRE_COMPLETO = {
    # Codigos DATASUS (6 digitos) e IBGE Completo (7 digitos)
    "120001": "Acrelândia",
    "1200013": "Acrelândia",
    
    "120005": "Assis Brasil",
    "1200054": "Assis Brasil",
    
    "120010": "Brasiléia",
    "1200104": "Brasiléia",
    
    "120013": "Bujari",
    "1200138": "Bujari",
    
    "120017": "Capixaba",
    "1200179": "Capixaba",
    
    "120020": "Cruzeiro do Sul",
    "1200203": "Cruzeiro do Sul",
    
    "120025": "Epitaciolândia",
    "1200252": "Epitaciolândia",
    
    "120030": "Feijó",
    "1200302": "Feijó",
    
    "120032": "Jordão",
    "1200328": "Jordão",
    
    "120033": "Mâncio Lima",
    "1200336": "Mâncio Lima",
    
    "120034": "Manoel Urbano",
    "1200344": "Manoel Urbano",
    
    "120035": "Marechal Thaumaturgo",
    "1200351": "Marechal Thaumaturgo",
    
    "120038": "Plácido de Castro",
    "1200385": "Plácido de Castro",
    
    "120039": "Porto Walter",
    "1200393": "Porto Walter",
    
    "120040": "Rio Branco (Capital)",
    "1200401": "Rio Branco (Capital)",
    
    "120042": "Rodrigues Alves",
    "1200427": "Rodrigues Alves",
    
    "120043": "Santa Rosa do Purus",
    "1200435": "Santa Rosa do Purus",
    
    "120045": "Senador Guiomard",
    "1200450": "Senador Guiomard",
    
    "120050": "Sena Madureira",
    "1200500": "Sena Madureira",
    
    "120060": "Tarauacá",
    "1200609": "Tarauacá",
    
    "120070": "Xapuri",
    "1200708": "Xapuri",
    
    "120080": "Porto Acre",
    "1200807": "Porto Acre"
}

# Mapeamento de UFs brasileiras
IBGE_UF_BRASIL = {
    "11": "Rondônia",
    "12": "Acre",
    "13": "Amazonas",
    "14": "Roraima",
    "15": "Pará",
    "16": "Amapá",
    "17": "Tocantins",
    "21": "Maranhão",
    "22": "Piauí",
    "23": "Ceará",
    "24": "Rio Grande do Norte",
    "25": "Paraíba",
    "26": "Pernambuco",
    "27": "Alagoas",
    "28": "Sergipe",
    "29": "Bahia",
    "31": "Minas Gerais",
    "32": "Espírito Santo",
    "33": "Rio de Janeiro",
    "35": "São Paulo",
    "41": "Paraná",
    "42": "Santa Catarina",
    "43": "Rio Grande do Sul",
    "50": "Mato Grosso do Sul",
    "51": "Mato Grosso",
    "52": "Goiás",
    "53": "Distrito Federal"
}

def obter_nome_municipio_completo(codigo: str) -> str:
    """
    Retorna o nome oficial do município brasileiro a partir do código IBGE.
    Suporta tanto códigos do Acre quanto códigos de outros estados.
    """
    cod_str = str(codigo).strip().replace(".0", "").zfill(6)
    
    # 1. Busca exata (6 dígitos ou 7 dígitos)
    if cod_str in IBGE_MUNICIPIOS_ACRE_COMPLETO:
        return IBGE_MUNICIPIOS_ACRE_COMPLETO[cod_str]
    
    # 2. Busca prefixo de 6 dígitos
    pref6 = cod_str[:6]
    if pref6 in IBGE_MUNICIPIOS_ACRE_COMPLETO:
        return IBGE_MUNICIPIOS_ACRE_COMPLETO[pref6]
        
    # 3. Pacientes vindos de outros Estados (fronteira RO, AM, etc.)
    uf_cod = cod_str[:2]
    if uf_cod in IBGE_UF_BRASIL:
        return f"Município de {IBGE_UF_BRASIL[uf_cod]} [{cod_str}]"
        
    return f"Município [{cod_str}]"
