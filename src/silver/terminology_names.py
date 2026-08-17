"""
Dicionario Central de De-Para e Nomes Legiveis do SUS (QIMED DataQore).
Mapeia codigos CID-10, SIGTAP, IBGE de Municipios, CNES de Hospitais e Tipos de Atendimento.
"""

# 1. Mapeamento de Municipios do Acre (IBGE)
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

# 2. Mapeamento de Hospitais (CNES)
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

# 3. Mapeamento de Doencas e Diagnosticos (CID-10)
CID10_NOMES = {
    "I21.9": "Infarto Agudo do Miocárdio não especificado (IAM)",
    "I21":   "Infarto Agudo do Miocárdio",
    "O80.0": "Parto Espontâneo Cefálico (Parto Normal)",
    "O80":   "Parto Único Espontâneo",
    "O82.0": "Parto por Cesariana Eletiva",
    "O82.1": "Parto por Cesariana de Emergência",
    "O82":   "Parto por Cesariana",
    "I64":   "Acidente Vascular Cerebral (AVC Agudo não especificado)",
    "I63.8": "Outros Infartos Cerebrais (AVC Isquêmico)",
    "I63":   "Infarto Cerebral (AVC Isquêmico)",
    "J18.9": "Pneumonia não especificada (Comunitária)",
    "J18.0": "Broncopneumonia não especificada",
    "J18":   "Pneumonia por Microrganismo não especificado",
    "J15.9": "Pneumonia Bacteriana não especificada",
    "J15":   "Pneumonia Bacteriana",
    "P07.3": "Prematuridade Extrema (Outros recém-nascidos pré-termo)",
    "P07":   "Transtornos relacionados com a gestação de curta duração",
    "I20.0": "Angina Instável (Síndrome Coronariana Aguda)",
    "I20":   "Angina Pectoris",
    "S12.2": "Fratura de Outras Vértebras Cervicais (Trauma Raquimedular)",
    "S12":   "Fratura do Pescoço/Vértebras Cervicais",
    "K80.8": "Cálculo na Vesícula Biliar sem colecistite (Colelitíase)",
    "K80":   "Colelitíase (Cálculos Biliares)",
    "K40.9": "Hérnia Inguinal Unilateral sem obstrução ou gangrena",
    "K40":   "Hérnia Inguinal",
    "N18.9": "Doença Renal Crônica não especificada (Insuficiência Renal)",
    "N18":   "Doença Renal Crônica",
    "A41.9": "Sepse não especificada (Choque Séptico / Infecção Generalizada)",
    "A41":   "Outras Sepses",
    "T88.8": "Outras Complicações de Cuidados Médicos e Cirúrgicos",
    "T88.9": "Complicação não especificada de Cuidado Cirúrgico/Médico",
    "I50.9": "Insuficiência Cardíaca não especificada",
    "I50.0": "Insuficiência Cardíaca Congestiva (ICC)",
    "I50":   "Insuficiência Cardíaca",
    "I10":   "Hipertensão Essencial (Pressão Alta)",
    "I15.9": "Hipertensão Secundária não especificada",
    "A90":   "Dengue Clássica",
    "A91":   "Febre Hemorrágica devida ao vírus do Dengue (Dengue Grave)",
    "E11.9": "Diabetes Mellitus Tipo 2 sem complicações",
    "E11.8": "Diabetes Mellitus Tipo 2 com complicações não especificadas",
    "E10.9": "Diabetes Mellitus Tipo 1 sem complicações",
    "E10":   "Diabetes Mellitus Tipo 1",
    "E11":   "Diabetes Mellitus Tipo 2",
    "N39.0": "Infecção do Trato Urinário (ITU)",
    "K35.9": "Apendicite Aguda não especificada",
    "K35":   "Apendicite Aguda",
    "K85.9": "Pancreatite Aguda não especificada",
    "K85":   "Pancreatite Aguda",
    "K92.8": "Outras Afecções do Aparelho Digestivo (Hemorragia Digestiva)",
    "C53.9": "Neoplasia Maligna do Colo do Útero (Câncer de Colo Uterino)",
    "C53":   "Neoplasia Maligna do Colo do Útero",
    "I42.2": "Outras Cardiomiopatias Hipertróficas (Cardiopatia Grave)",
    "D75.9": "Doença do Sangue e dos Órgãos Hematopoéticos não especificada",
    "S01.8": "Ferimento de Outras Partes da Cabeça (Trauma Craniofacial)",
    "O23.4": "Infecção não especificada do Trato Urinário na Gravidez",
    "F31.2": "Transtorno Afetivo Bipolar (Episódio Maníaco com Sintomas Psicóticos)",
    "A46":   "Erisipela (Infecção Cutânea Aguda)",
    "N13.2": "Hidronefrose com Obstrução por Cálculo Renal",
    "N73.9": "Doença Inflamatória Pélvica Feminina não especificada",
    "A53.9": "Sífilis não especificada",
    "A49.9": "Infecção Bacteriana de Localização não especificada",
    "Z53.9": "Procedimento não Realizado por Motivo não especificado",
    "R02":   "Gangrena não classificada em outra parte (Pé Diabético / Necrose)",
    "L98.8": "Outros Transtornos Especificados da Pele e Tecido Subcutâneo",
    "K59.8": "Outros Transtornos Funcionais do Intestino (Obstrução Intestinal)"
}

# 4. Mapeamento de Procedimentos Hospitalares e Ambulatoriais (SIGTAP)
SIGTAP_NOMES = {
    "0301060088": "Atendimento Médico em Unidade de Pronto Atendimento / Urgência",
    "0303010010": "Tratamento Clínico de Dengue",
    "0303010037": "Tratamento de Outras Doenças Infecciosas e Parasitárias",
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
    """Retorna o nome completo e legivel do diagnostico CID-10."""
    cod = str(codigo).strip().upper()
    if cod in CID10_NOMES:
        return CID10_NOMES[cod]
    base = cod.split(".")[0]
    if base in CID10_NOMES:
        return CID10_NOMES[base]
    if capitulo:
        return f"Diagnóstico [{cod}] ({capitulo})"
    return f"Diagnóstico [{cod}]"

def resolver_nome_procedimento(codigo: str) -> str:
    """Retorna o nome completo e legivel do procedimento SIGTAP."""
    cod = str(codigo).strip().replace(".", "").replace("-", "")
    if cod in SIGTAP_NOMES:
        return SIGTAP_NOMES[cod]
    return f"Procedimento SUS [{cod}]"

def resolver_nome_municipio(codigo: str) -> str:
    """Retorna o nome oficial do municipio brasileiro via codigo IBGE."""
    cod = str(codigo).strip()[:6]
    if cod in IBGE_MUNICIPIOS_AC:
        return IBGE_MUNICIPIOS_AC[cod]
    return f"Município IBGE [{cod}]"

def resolver_nome_hospital(cnes_ou_org_id: str) -> str:
    """Retorna o nome oficial do hospital via codigo CNES."""
    val = str(cnes_ou_org_id).replace("org_cnes_", "").strip()
    if val in CNES_HOSPITAIS_AC:
        return CNES_HOSPITAIS_AC[val]
    return f"Hospital CNES [{val}]"
