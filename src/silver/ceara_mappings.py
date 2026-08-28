"""
Mapeamento de Terminologias Oficiais de Saúde do Estado do Ceará (QIMED DataQore).
Contém:
1. Mapeamento de Todos os 184 Municípios do Estado do Ceará (IBGE 6 e 7 dígitos).
2. Mapeamento dos Principais Complexos Hospitalares, Policlínicas e UPAs do Ceará (CNES).
3. Mapeamento Amplo de Diagnósticos e Doenças (CID-10).
"""

# ==============================================================================
# 1. TODOS OS 184 MUNICÍPIOS DO ESTADO DO CEARÁ (IBGE)
# ==============================================================================
IBGE_MUNICIPIOS_CEARA = {
    "230010": "Abaiara",
    "230015": "Acarape",
    "230020": "Acaraú",
    "230030": "Acopiara",
    "230040": "Aiuaba",
    "230050": "Alcântaras",
    "230060": "Altaneira",
    "230070": "Alto Santo",
    "230075": "Amontada",
    "230080": "Antonina do Norte",
    "230090": "Apuiarés",
    "230100": "Aquiraz",
    "230110": "Aracati",
    "230120": "Aracoiaba",
    "230125": "Ararendá",
    "230130": "Araripe",
    "230140": "Aratuba",
    "230150": "Arneiroz",
    "230160": "Assaré",
    "230170": "Aurora",
    "230180": "Baixio",
    "230185": "Banabuiú",
    "230190": "Barbalha",
    "230195": "Barreira",
    "230200": "Barro",
    "230205": "Barroquinha",
    "230210": "Baturité",
    "230220": "Beberibe",
    "230230": "Bela Cruz",
    "230240": "Boa Viagem",
    "230250": "Brejo Santo",
    "230260": "Camocim",
    "230270": "Campos Sales",
    "230280": "Canindé",
    "230290": "Capistrano",
    "230300": "Caridade",
    "230310": "Cariré",
    "230320": "Caririaçu",
    "230330": "Cariús",
    "230340": "Carnaubal",
    "230350": "Cascavel",
    "230360": "Catarina",
    "230365": "Catunda",
    "230370": "Caucaia",
    "230380": "Cedro",
    "230390": "Chaval",
    "230393": "Choró",
    "230395": "Chorozinho",
    "230400": "Coreaú",
    "230410": "Crateús",
    "230420": "Crato",
    "230423": "Croatá",
    "230425": "Cruz",
    "230426": "Deputado Irapuan Pinheiro",
    "230427": "Ereré",
    "230428": "Eusébio",
    "230430": "Farias Brito",
    "230435": "Forquilha",
    "230440": "Fortaleza (Capital)",
    "230445": "Fortim",
    "230450": "Frecheirinha",
    "230460": "General Sampaio",
    "230465": "Graça",
    "230470": "Granja",
    "230480": "Granjeiro",
    "230490": "Groaíras",
    "230495": "Guaiúba",
    "230500": "Guaraciaba do Norte",
    "230510": "Guaramiranga",
    "230520": "Hidrolândia",
    "230523": "Horizonte",
    "230526": "Ibaretama",
    "230530": "Ibiapina",
    "230533": "Ibicuitinga",
    "230535": "Icapuí",
    "230540": "Icó",
    "230550": "Iguatu",
    "230560": "Independência",
    "230565": "Ipaporanga",
    "230570": "Ipaumirim",
    "230580": "Ipu",
    "230590": "Ipueiras",
    "230600": "Iracema",
    "230610": "Irauçuba",
    "230620": "Itaiçaba",
    "230625": "Itaitinga",
    "230630": "Itapagé",
    "230640": "Itapipoca",
    "230650": "Itapiúna",
    "230655": "Itarema",
    "230660": "Itatira",
    "230670": "Jaguaretama",
    "230680": "Jaguaribara",
    "230690": "Jaguaribe",
    "230700": "Jaguaruana",
    "230710": "Jardim",
    "230720": "Jati",
    "230725": "Jijoca de Jericoacoara",
    "230730": "Juazeiro do Norte",
    "230740": "Jucás",
    "230750": "Lavras da Mangabeira",
    "230760": "Limoeiro do Norte",
    "230763": "Madalena",
    "230765": "Maracanaú",
    "230770": "Maranguape",
    "230780": "Marco",
    "230790": "Martinópole",
    "230800": "Massapê",
    "230810": "Mauriti",
    "230820": "Meruoca",
    "230830": "Milagres",
    "230835": "Milhã",
    "230837": "Miraíma",
    "230840": "Missão Velha",
    "230850": "Mombaça",
    "230860": "Monsenhor Tabosa",
    "230870": "Morada Nova",
    "230880": "Moraújo",
    "230890": "Morrinhos",
    "230900": "Mucambo",
    "230910": "Mulungu",
    "230920": "Nova Olinda",
    "230930": "Nova Russas",
    "230940": "Novo Oriente",
    "230945": "Ocara",
    "230950": "Orós",
    "230960": "Pacajus",
    "230970": "Pacatuba",
    "230980": "Pacoti",
    "230990": "Pacujá",
    "231000": "Palhano",
    "231010": "Palmácia",
    "231020": "Paracuru",
    "231025": "Paraipaba",
    "231030": "Parambu",
    "231040": "Paramoti",
    "231050": "Pedra Branca",
    "231060": "Penaforte",
    "231070": "Pentecoste",
    "231080": "Pereiro",
    "231085": "Pindoretama",
    "231090": "Piquet Carneiro",
    "231095": "Pires Ferreira",
    "231100": "Poranga",
    "231110": "Porteiras",
    "231120": "Potengi",
    "231123": "Potiretama",
    "231126": "Quiterianópolis",
    "231130": "Quixadá",
    "231135": "Quixelô",
    "231140": "Quixeramobim",
    "231150": "Quixeré",
    "231160": "Redenção",
    "231170": "Reriutaba",
    "231180": "Russas",
    "231190": "Saboeiro",
    "231195": "Salitre",
    "231200": "Santana do Acaraú",
    "231210": "Santana do Cariri",
    "231220": "Santa Quitéria",
    "231230": "São Benedito",
    "231240": "São Gonçalo do Amarante",
    "231250": "São João do Jaguaribe",
    "231260": "São Luís do Curu",
    "231270": "Senador Pompeu",
    "231280": "Senador Sá",
    "231290": "Sobral",
    "231300": "Solonópole",
    "231310": "Tabuleiro do Norte",
    "231320": "Tamboril",
    "231325": "Tarrafas",
    "231330": "Tauá",
    "231335": "Tejuçuoca",
    "231340": "Tianguá",
    "231350": "Trairi",
    "231355": "Tururu",
    "231360": "Ubajara",
    "231370": "Umari",
    "231375": "Umirim",
    "231380": "Uruburetama",
    "231390": "Uruoca",
    "231395": "Varjota",
    "231400": "Várzea Alegre",
    "231410": "Viçosa do Ceará"
}

# ==============================================================================
# 2. PRINCIPAIS HOSPITAIS E COMPLEXOS DE SAÚDE DO CEARÁ (CNES)
# ==============================================================================
CNES_HOSPITAIS_CEARA = {
    # Hospitais Terciários e Especializados de Fortaleza (Rede SESA / SUS)
    "2479214": "Hospital de Messejana Dr. Carlos Alberto Studart Gomes (Cardiopulmonar)",
    "2497654": "Hospital Geral de Fortaleza (HGF - Alta Complexidade)",
    "2529149": "Instituto Dr. José Frota (IJF - Centro de Trauma e Urgência)",
    "2499363": "Hospital Infantil Albert Sabin (HIAS - Pediatria Terciária)",
    "2499355": "Hospital Geral Dr. César Cals (HGCC - Maternidade e Clínica Geral)",
    "2480662": "Hospital Geral Dr. Waldemar Alcântara (HGWA - Retaguarda Clínica)",
    "2561492": "Hospital São José de Doenças Infecciosas (HSJ)",
    "2479427": "Hospital de Saúde Mental Prof. Frota Pinto (HSMM)",
    "2479532": "Maternidade Escola Assis Chateaubriand (MEAC / UFC)",
    "2479524": "Hospital Universitário Walter Cantídio (HUWC / UFC / EBSERH)",
    "2528738": "Hospital Distrital Gonzaga Mota - Messejana",
    "2528746": "Hospital Distrital Gonzaga Mota - Barra do Ceará",
    "2528754": "Hospital Distrital Maria José Barroso - Frotinha Parangaba",
    "2528762": "Hospital Distrital Evandro Ayres de Moura - Frotinha Antônio Bezerra",
    "2528770": "Hospital Distrital Edmilson Barros de Oliveira - Frotinha Messejana",
    "6877443": "Hospital da Mulher de Fortaleza Dra. Zilda Arns Neumann",
    "2372967": "Instituto do Câncer do Ceará (ICC / Hospital Haroldo Juaçaba)",

    # Hospitais Regionais do Estado do Ceará (Interior)
    "6848710": "Hospital Regional do Cariri (HRC - Juazeiro do Norte)",
    "6877443": "Hospital Regional Norte (HRN - Sobral)",
    "7320418": "Hospital Regional do Sertão Central (HRSC - Quixeramobim)",
    "7772645": "Hospital Regional Vale do Jaguaribe (HRVJ - Limoeiro do Norte)",
    "3021114": "Santa Casa de Misericórdia de Sobral",
    "2563339": "Hospital Municipal e Maternidade de Caucaia",
    "2414872": "Hospital Municipal Dr. João Elísio de Holanda (Maracanaú)",
    "2478080": "Hospital Municipal Dr. Amadeu Furtado (Santa Quitéria)",
    "2499347": "Hospital Municipal Dr. Eudásio Barroso (Quixadá)",
    "2528789": "Hospital e Maternidade São Lucas (Juazeiro do Norte)",
    "2528797": "Hospital Geral de Tianguá",
    "2528800": "Hospital Maternidade Jesus Maria José (Quixadá)",
    "5018110": "Hospital Municipal Dr. José Evangelista de Oliveira (Ipu)",
    "2425300": "Hospital Municipal de Guaraciaba do Norte",
    "2426579": "Hospital Municipal de Croatá",
    "2563681": "Hospital Regional de Crateús",
    "2481073": "Hospital Municipal São Lucas (Crateús)",
    "2723611": "Centro de Especialidades Odontológicas / Policlínica de Ipu",
    "4155769": "Laboratório Central Municipal de Ipu",
    "5990270": "Centro de Saúde / Policlínica Municipal de Ipu"
}

# ==============================================================================
# 3. DICIONÁRIO EXPANDIDO DE PATOLOGIAS E CIDs (CID-10)
# ==============================================================================
CID10_EXPANDIDO = {
    # Obstetrícia e Partos
    "O42.0": "Ruptura Prematura das Membranas (Bolsa Rota no Início do Trabalho)",
    "O42":   "Ruptura Prematura de Membranas (Bolsa Rota)",
    "O80.0": "Parto Normal Espontâneo Cefálico",
    "O80":   "Parto Único Espontâneo",
    "O82.0": "Parto por Cesariana Eletiva",
    "O82":   "Parto por Cesariana",

    # Doenças Cardiovasculares
    "I24.8": "Outras Formas de Doença Isquêmica Aguda do Coração (Síndrome Coronariana)",
    "I24":   "Outras Doenças Isquêmicas Agudas do Coração",
    "I21.9": "Infarto Agudo do Miocárdio (IAM não especificado)",
    "I21.0": "IAM de Parede Anterior",
    "I21.1": "IAM de Parede Inferior",
    "I21":   "Infarto Agudo do Miocárdio",
    "I20.0": "Angina Instável",
    "I20":   "Angina Pectoris",
    "I50.0": "Insuficiência Cardíaca Congestiva (ICC)",
    "I50.9": "Insuficiência Cardíaca não especificada",
    "I50":   "Insuficiência Cardíaca",
    "I10":   "Hipertensão Essencial (Pressão Alta)",
    "I11.0": "Doença Cardíaca Hipertensiva com Insuficiência Cardíaca",
    "I11":   "Doença Cardíaca Hipertensiva",
    "I42.0": "Cardiomiopatia Dilatada",
    "I48":   "Fibrilação e Flutter Atrial",

    # Doenças Cerebrovasculares e Neurológicas
    "I64":   "Acidente Vascular Cerebral (AVC não especificado)",
    "I63.9": "Infarto Cerebral Isquêmico (AVC Isquêmico)",
    "I63":   "Infarto Cerebral",
    "I61.9": "Hemorragia Intracerebral (AVC Hemorrágico)",
    "I61":   "Hemorragia Intracerebral",
    "G04.9": "Encefalite / Mielite não especificada",
    "G04":   "Encefalite, Mielite e Encefalomielite",
    "G40.9": "Epilepsia / Crise Convulsiva não especificada",
    "G40":   "Epilepsia",

    # Doenças Respiratórias
    "J18.9": "Pneumonia Comunitária não especificada",
    "J18.0": "Broncopneumonia",
    "J18":   "Pneumonia por Microrganismo não especificado",
    "J15.9": "Pneumonia Bacteriana",
    "J44.1": "DPOC com Exacerbação Aguda",
    "J44":   "Doença Pulmonar Obstrutiva Crônica (DPOC)",
    "J45.0": "Asma Predominantemente Alérgica",
    "J45.9": "Asma não especificada",
    "J45":   "Asma",
    "J20.9": "Bronquite Aguda",

    # Doenças Infecciosas e Parasitárias
    "A41.9": "Septicemia não especificada (Sepse / Choque Séptico)",
    "A41":   "Outras Septicemias",
    "A90":   "Dengue Clássica",
    "A91":   "Febre Hemorrágica do Dengue (Dengue Grave)",
    "A92.0": "Febre de Chikungunya",
    "A09":   "Gastroenterite e Colite de Origem Infecciosa (Diarreia Aguda)",
    "A00":   "Cólera",
    "A46":   "Erisipela",
    "B24":   "Doença pelo Vírus da Imunodeficiência Humana (HIV/AIDS)",
    "A15.0": "Tuberculose Pulmonar",

    # Saúde Materno-Infantil e Obstetrícia
    "O80.0": "Parto Normal Espontâneo Cefálico",
    "O80":   "Parto Único Espontâneo",
    "O82.0": "Parto por Cesariana Eletiva",
    "O82.1": "Parto por Cesariana de Emergência",
    "O82":   "Parto por Cesariana",
    "O24.4": "Diabetes Mellitus na Gravidez (Gestacional)",
    "O14.9": "Pré-Eclâmpsia não especificada",
    "O15.0": "Eclâmpsia na Gravidez",
    "P07.3": "Prematuridade / Recém-nascido Pré-termo",
    "P22.0": "Síndrome de Angústia Respiratória do Recém-Nascido",
    # Capítulos Z (Fatores que influenciam o estado de saúde e contato com serviços)
    "Z30":   "Planejamento Familiar / Anticoncepção / Laqueadura",
    "Z30.0": "Aconselhamento e Orientação sobre Contracepção",
    "Z30.2": "Esterilização Cirúrgica / Laqueadura Tubária Pós-Parto",
    "Z38":   "Recém-Nascido Vivo Nascido em Hospital",
    "Z38.0": "Recém-Nascido Único Nascido em Hospital",
    "Z39":   "Assistência e Exame no Pós-Parto Imediato",
    "Z39.2": "Acompanhamento de Rotina Pós-Parto",
    "Z53":   "Procedimento Não Realizado por Contraindicação Médica",
    "Z53.9": "Procedimento Não Realizado por Motivo Não Especificado",
    "Z00":   "Exame Médico Geral e Rotina de Saúde",
    "Z01":   "Outros Exames Médicos Especiais",
    "Z51":   "Cuidados Médicos Adicionais (Quimioterapia / Radioterapia)",

    # Sintomas, Sinais e Achados Anormais (Capítulo R)
    "R10":   "Dor Abdominal e Pélvica Aguda (Abdômen Agudo)",
    "R10.0": "Abdômen Agudo Cirúrgico",
    "R10.4": "Outras Dores Abdominais Não Especificadas",
    "R07":   "Dor Torácica / Dor no Peito Suspeita de Cardiopatia",
    "R07.4": "Dor Torácica Não Especificada",
    "R50":   "Febre de Origem Desconhecida / Febre Aguda",
    "R50.9": "Febre Não Especificada",
    "R06":   "Anormalidades da Respiração / Dispneia Aguda",
    "R55":   "Síncope e Colapso (Desmaio / Perda Transitória da Consciência)",
    "R56":   "Convulsões e Crises Convulsivas",
    "R65":   "Síndrome de Resposta Inflamatória Sistêmica (SIRS)",

    # Doenças Endócrinas e Metabólicas
    "E11.9": "Diabetes Mellitus Tipo 2 sem Complicações",
    "E11.0": "Diabetes Mellitus Tipo 2 com Coma / Cetoacidose",
    "E11.5": "Diabetes Tipo 2 com Complicações Circulatórias Periféricas (Pé Diabético)",
    "E11":   "Diabetes Mellitus Tipo 2",
    "E10.9": "Diabetes Mellitus Tipo 1 sem Complicações",
    "E10":   "Diabetes Mellitus Tipo 1",
    "E86":   "Depleção de Volume (Desidratação Grave)",

    # Aparelho Digestivo e Cirurgia Geral
    "K35.9": "Apendicite Aguda",
    "K35":   "Apendicite Aguda",
    "K80.0": "Cálculo da Vesícula Biliar com Colecistite Aguda",
    "K80":   "Colelitíase (Pedra na Vesícula)",
    "K85.9": "Pancreatite Aguda",
    "K85":   "Pancreatite Aguda",
    "K40.9": "Hérnia Inguinal Unilateral",
    "K40":   "Hérnia Inguinal",
    "K92.2": "Hemorragia Gastrointestinal não especificada",
    "K92":   "Hemorragia e Doenças do Aparelho Digestivo",

    # Aparelho Geniturinário e Renal
    "N39.0": "Infecção do Trato Urinário (ITU)",
    "N39":   "Infecção e Transtornos do Trato Urinário (ITU)",
    "N18.9": "Doença Renal Crônica (DRC Terminal)",
    "N18.5": "Doença Renal Crônica Estágio 5 (Dialítica)",
    "N18":   "Doença Renal Crônica",
    "N20.0": "Cálculo do Rim (Nefrolitíase)",
    "N20":   "Cálculo do Rim e Ureter",

    # Traumatologia e Causas Externas
    "S06.9": "Traumatismo Intracraniano não especificado (TCE)",
    "S06":   "Traumatismo Intracraniano (TCE)",
    "S72.0": "Fratura do Colo do Fêmur",
    "S72":   "Fratura do Fêmur",
    "T88.8": "Outras Complicações de Procedimentos Cirúrgicos e Médicos",
    "T88":   "Complicações de Cuidados Cirúrgicos e Médicos"
}


def resolver_nome_municipio_ce(codigo: str) -> str:
    """Retorna o nome oficial do município cearense de forma resiliente."""
    if not codigo:
        return "Município Não Informado"
    
    val = str(codigo).strip()
    
    # 1. Se o valor já contém o nome de algum município do Ceará (ex: "Quixadá", "Sobral", "Fortaleza", "Santa Quitéria")
    for cod, nome in IBGE_MUNICIPIOS_CEARA.items():
        if nome.lower() in val.lower() or val.lower().startswith(nome[:5].lower()):
            return nome

    # 2. Se contém código numérico IBGE de 6 dígitos do Ceará (23XXXX)
    import re
    m = re.search(r'\b(23\d{4})\b', val)
    if m and m.group(1) in IBGE_MUNICIPIOS_CEARA:
        return IBGE_MUNICIPIOS_CEARA[m.group(1)]

    # 3. Limpeza de caracteres não numéricos para extração de código
    cod_clean = re.sub(r'[^\d]', '', val)[:6]
    if cod_clean in IBGE_MUNICIPIOS_CEARA:
        return IBGE_MUNICIPIOS_CEARA[cod_clean]
    
    # 4. Se é um nome textual limpo
    if any(c.isalpha() for c in val) and not val.startswith("Município IBGE"):
        return val

    return f"Município IBGE [{cod_clean if cod_clean else val}]"


def resolver_nome_hospital_ce(cnes_ou_org_id: str) -> str:
    """Retorna o nome oficial do hospital cearense ou fallback para CNES."""
    if not cnes_ou_org_id:
        return "Hospital Não Informado"
    
    val = str(cnes_ou_org_id).replace("org_cnes_", "").strip()
    if any(c.isalpha() for c in val) and not val.startswith("Hospital CNES"):
        return val

    cod7 = val.zfill(7)
    if cod7 in CNES_HOSPITAIS_CEARA:
        return CNES_HOSPITAIS_CEARA[cod7]
    return f"Hospital CNES [{cod7}]"


def resolver_nome_doenca_expandido(codigo: str, capitulo: str = "") -> str:
    """Retorna a descrição médica e clínica da doença a partir do CID-10."""
    if not codigo:
        return "Diagnóstico Não Informado"
    cod = str(codigo).strip().upper()
    if cod in CID10_EXPANDIDO:
        return CID10_EXPANDIDO[cod]
    base = cod.split(".")[0]
    if base in CID10_EXPANDIDO:
        return CID10_EXPANDIDO[base]
    if capitulo and capitulo != "chapter":
        return f"{cod} ({capitulo})"
    return f"CID {cod}"
