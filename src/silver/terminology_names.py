"""
Dicionário Central de De-Para e Nomes Legíveis do SUS (QIMED DataQore).
Mapeia códigos CID-10, SIGTAP, IBGE de Municípios (Ceará + Acre + Brasil), CNES de Hospitais e Tipos de Atendimento.
"""
from src.silver.ceara_mappings import (
    IBGE_MUNICIPIOS_CEARA,
    CNES_HOSPITAIS_CEARA,
    CID10_EXPANDIDO,
    resolver_nome_municipio_ce,
    resolver_nome_hospital_ce,
    resolver_nome_doenca_expandido
)

# 1. Mapeamento de Municípios do Acre (IBGE)
IBGE_MUNICIPIOS_AC = {
    "120001": "Acrelândia",
    "120005": "Assis Brasil",
    "120010": "Brasiléia",
    "120013": "Bujari",
    "120017": "Capixaba",
    "120020": "Cruzeiro do Sul",
    "120025": "Epitaciolândia",
    "120030": "Feijó",
    "120032": "Jordão",
    "120033": "Mâncio Lima",
    "120034": "Manoel Urbano",
    "120035": "Marechal Thaumaturgo",
    "120038": "Plácido de Castro",
    "120039": "Porto Walter",
    "120040": "Rio Branco (Capital)",
    "120042": "Rodrigues Alves",
    "120043": "Santa Rosa do Purus",
    "120045": "Senador Guiomard",
    "120050": "Sena Madureira",
    "120060": "Tarauacá",
    "120070": "Xapuri",
    "120080": "Porto Acre"
}

# 2. Mapeamento de Hospitais do Acre (CNES)
CNES_HOSPITAIS_AC = {
    "2001578": "Hospital de Clínicas do Acre (HC / Alta Complexidade)",
    "5336171": "Pronto-Socorro de Rio Branco (Huerb / Urgência e Trauma)",
    "2000733": "Maternidade Bárbara Heliodora (Saúde da Mulher / Neonatologia)",
    "2002078": "Hospital Regional do Alto Acre (Brasiléia)",
    "2001586": "Unidade Oncológica e Cuidados Paliativos (Unacon)",
    "2000725": "Hospital Regional do Juruá (Cruzeiro do Sul)",
    "2000296": "Hospital Dr. João Canuto (Tarauacá)",
    "2001500": "Hospital Sanson Pereira (Sena Madureira)",
    "2000121": "Hospital Municipal de Feijó",
    "2000865": "Hospital Municipal de Xapuri",
    "2001594": "Hospital Geral de Marechal Thaumaturgo",
    "2000172": "Hospital Geral de Manoel Urbano",
    "2000636": "Unidade Mista de Feijó"
}

# 3. Procedimentos SIGTAP
SIGTAP_NOMES = {
    "0301060088": "Atendimento Médico em Unidade de Pronto Atendimento / Urgência",
    "0301060061": "Atendimento de Urgência com Observação até 24 Horas",
    "0301060029": "Atendimento Médico em UPA 24h",
    "0301010048": "Consulta Especializada de Nível Superior (Não Médica)",
    "0205020143": "Ultrassonografia Doppler de Fluxo Obstétrico",
    "0303010010": "Tratamento Clínico de Dengue",
    "0303010037": "Tratamento de Doenças Infecciosas e Parasitárias",
    "0303010126": "Tratamento de Doenças Sexualmente Transmissíveis",
    "0303010142": "Tratamento de Meningites e Encefalites Virais",
    "0303020083": "Tratamento de Doenças do Sangue e Órgãos Hematopoéticos",
    "0303030038": "Tratamento Clínico de Diabetes Mellitus Descompensado",
    "0303040149": "Tratamento Clínico de Acidente Vascular Cerebral (AVC)",
    "0303060034": "Tratamento de Insuficiência Cardíaca e Cardiopatias Graves",
    "0303060107": "Tratamento Clínico de Crise Hipertensiva Grave",
    "0303070102": "Tratamento de Doenças do Aparelho Digestivo em Unidade Hospitalar",
    "0303070129": "Tratamento Clínico de Doenças do Pâncreas (Pancreatite)",
    "0303100044": "Tratamento de Infecção Urinária na Gestação",
    "0303150025": "Tratamento Clínico de Doenças Renais Agudas",
    "0303150033": "Tratamento Clínico de Doenças Ginecológicas Agudas",
    "0303150041": "Tratamento de Cólica Nefrética e Cálculo Renal",
    "0303150050": "Tratamento Clínico de Infecções Urinárias Altas / Pielonefrite",
    "0303160020": "Tratamento Intensivo / Semi-Intensivo de Recém-Nascido",
    "0303160039": "Tratamento de Prematuridade e Desconforto Respiratório Neonatal",
    "0303160063": "Ventilação Mecânica e Suporte Respiratório Neonatal",
    "0303170140": "Tratamento em Psiquiatria / Crise Psiquiátrica Aguda",
    "0304100021": "Tratamento Oncológico Clínico e Cuidados Paliativos",
    "0310010039": "Parto Normal Único sem Distócia",
    "0310010047": "Parto por Cesariana com Laqueadura Tubária",
    "0310010055": "Parto por Cesariana em Gestação de Alto Risco",
    "0401020053": "Desbridamento Cirúrgico de Tecidos Desvitalizados",
    "0407020101": "Laparotomia Exploradora Cirúrgica",
    "0407030026": "Apendicectomia Cirúrgica (Remoção do Apêndice)",
    "0407030034": "Colecistectomia Cirúrgica (Remoção da Vesícula Biliar)",
    "0407040102": "Herniorrafia Inguinal Cirúrgica (Correção de Hérnia)",
    "0408050080": "Tratamento Cirúrgico de Fratura / Redução e Fixação Óssea",
    "0415040035": "Cirurgia Reparadora e Tratamento de Feridas Complexas",
    "0201010010": "Exame Laboratorial / Coleta de Material Biológico",
    "0301010072": "Consulta Médica Especializada em Ambulatório (SIA)"
}


def resolver_nome_doenca(codigo: str, capitulo: str = "") -> str:
    """Retorna o nome completo e legível do diagnóstico CID-10 (Ceará + Brasil)."""
    return resolver_nome_doenca_expandido(codigo, capitulo)


def resolver_nome_procedimento(codigo: str) -> str:
    """Retorna o nome completo e legível do procedimento SIGTAP."""
    cod = str(codigo).strip().replace(".", "").replace("-", "")
    if cod in SIGTAP_NOMES:
        return SIGTAP_NOMES[cod]
    return f"Procedimento SUS [{cod}]"


def resolver_nome_municipio(codigo: str) -> str:
    """Retorna o nome oficial do município brasileiro (Ceará, Acre ou demais estados)."""
    return resolver_nome_municipio_ce(codigo)


def resolver_nome_hospital(cnes_ou_org_id: str) -> str:
    """Retorna o nome oficial do hospital (Ceará, Acre ou demais estados)."""
    return resolver_nome_hospital_ce(cnes_ou_org_id)
