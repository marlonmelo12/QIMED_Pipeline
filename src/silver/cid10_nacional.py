"""
Dicionário e Resolvedor Integral de Diagnósticos CID-10 da OMS / DATASUS.
Cobre todos os 22 Capítulos da CID-10 e subcategorias para tradução clínica em português.
"""
from typing import Tuple

# 1. Capítulos Gerais da CID-10
CAPITULOS_CID10 = {
    "I": ("A00-B99", "Algumas doenças infecciosas e parasitárias"),
    "II": ("C00-D48", "Neoplasias (tumores malignos e benignos)"),
    "III": ("D50-D89", "Doenças do sangue e dos órgãos hematopoéticos"),
    "IV": ("E00-E90", "Doenças endócrinas, nutricionais e metabólicas"),
    "V": ("F00-F99", "Transtornos mentais e comportamentais"),
    "VI": ("G00-G99", "Doenças do sistema nervoso"),
    "VII": ("H00-H59", "Doenças do olho e anexos"),
    "VIII": ("H60-H95", "Doenças do ouvido e da apófise mastoide"),
    "IX": ("I00-I99", "Doenças do aparelho circulatório (Cardiovasculares / AVC)"),
    "X": ("J00-J99", "Doenças do aparelho respiratório"),
    "XI": ("K00-K93", "Doenças do aparelho digestivo"),
    "XII": ("L00-L99", "Doenças da pele e do tecido subcutâneo"),
    "XIII": ("M00-M99", "Doenças do sistema osteomuscular e do tecido conjuntivo"),
    "XIV": ("N00-N99", "Doenças do aparelho geniturinário"),
    "XV": ("O00-O99", "Gravidez, parto e puerpério (Obstetrícia)"),
    "XVI": ("P00-P96", "Algumas afecções originadas no período perinatal (Neonatologia)"),
    "XVII": ("Q00-Q99", "Malformações congênitas, deformidades e anomalias cromossômicas"),
    "XVIII": ("R00-R99", "Sintomas, sinais e achados anormais não classificados"),
    "XIX": ("S00-T98", "Lesões, envenenamentos e consequências de causas externas (Trauma)"),
    "XX": ("V01-Y98", "Causas externas de morbidade e de mortalidade"),
    "XXI": ("Z00-Z99", "Fatores que influenciam o estado de saúde e contato com serviços (Planejamento Familiar)"),
    "XXII": ("U00-U99", "Códigos para propósitos especiais (COVID-19)")
}

# 2. Categorias e Doenças Mais Frequentes no DATASUS
CID10_DIAGNOSTICOS_MESTRE = {
    # Circulatório
    "I21": "Infarto Agudo do Miocárdio (IAM)",
    "I21.9": "Infarto Agudo do Miocárdio não especificado",
    "I20": "Angina Pectoris (Dor Coronariana)",
    "I50": "Insuficiência Cardíaca Congestiva (ICC)",
    "I50.0": "Insuficiência Cardíaca Congestiva",
    "I10": "Hipertensão Arterial Sistêmica (Pressão Alta)",
    "I11": "Doença Cardíaca Hipertensiva",
    "I64": "Acidente Vascular Cerebral (AVC não especificado)",
    "I63": "Infarto Cerebral Isquêmico (AVC Isquêmico)",
    "I61": "Hemorragia Intracerebral (AVC Hemorrágico)",

    # Respiratório
    "J18": "Pneumonia por Microrganismo não especificado",
    "J18.9": "Pneumonia Comunitária",
    "J15": "Pneumonia Bacteriana",
    "J12": "Pneumonia Viral",
    "J44": "Doença Pulmonar Obstrutiva Crônica (DPOC)",
    "J45": "Asma / Crise Asmática",
    "J20": "Bronquite Aguda",
    "J06": "Infecção Aguda das Vias Aéreas Superiores (IVAS)",

    # Infecciosas
    "A41": "Sepse / Septicemia / Choque Séptico",
    "A41.9": "Sepse não especificada",
    "A90": "Dengue Clássica",
    "A91": "Febre Hemorrágica do Dengue (Dengue Grave)",
    "A09": "Gastroenterite e Diarreia Aguda Infecciosa",
    "A46": "Erisipela",
    "B24": "Doença pelo Vírus HIV/AIDS",
    "A15": "Tuberculose Respiratória",

    # Obstetrícia e Partos
    "O80": "Parto Normal Único Espontâneo",
    "O80.0": "Parto Normal Espontâneo Cefálico",
    "O82": "Parto por Cesariana",
    "O82.0": "Parto por Cesariana Eletiva",
    "O42": "Ruptura Prematura de Membranas (Bolsa Rota)",
    "O42.0": "Bolsa Rota no Início do Trabalho de Parto",
    "O14": "Pré-Eclâmpsia na Gravidez",
    "O15": "Eclâmpsia",
    "P07": "Prematuridade / Recém-Nascido Pré-termo",
    "P22": "Desconforto Respiratório do Recém-Nascido",

    # Planejamento Familiar e Contato com Serviços
    "Z30": "Planejamento Familiar / Anticoncepção / Laqueadura",
    "Z30.2": "Esterilização Cirúrgica / Laqueadura Tubária",
    "Z38": "Recém-Nascido Vivo Nascido em Hospital",
    "Z39": "Acompanhamento e Cuidados Pós-Parto",
    "Z53": "Procedimento Não Realizado por Contraindicação",

    # Sintomas e Abdômen Agudo
    "R10": "Dor Abdominal e Pélvica Aguda (Abdômen Agudo)",
    "R07": "Dor Torácica / Dor no Peito",
    "R50": "Febre Aguda de Origem Desconhecida",
    "R06": "Dispneia / Falta de Ar Aguda",
    "R55": "Síncope e Desmaio",

    # Digestivo, Renal e Traumatologia
    "K35": "Apendicite Aguda",
    "K80": "Colelitíase (Pedra na Vesícula)",
    "K40": "Hérnia Inguinal",
    "K85": "Pancreatite Aguda",
    "N39": "Infecção do Trato Urinário (ITU)",
    "N18": "Doença Renal Crônica (DRC)",
    "S06": "Traumatismo Cranioencefálico (TCE)",
    "S72": "Fratura do Fêmur",
    "E11": "Diabetes Mellitus Tipo 2",
    "E10": "Diabetes Mellitus Tipo 1"
}

import os
import pandas as pd

# Carregamento da Dimensao Completa dos 14.257 Diagnosticos CID-10 do DATASUS
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PARQUET_CID = os.path.join(_BASE_DIR, "config", "dim_cid10_datasus.parquet")

_CID10_DATASUS_MAP: dict = {}

if os.path.exists(_PARQUET_CID):
    try:
        df_c = pd.read_parquet(_PARQUET_CID)
        for _, r in df_c.iterrows():
            c_code = str(r["cid10_code"]).strip().upper().replace(".", "")
            c_desc = str(r["disease_name"]).strip()
            _CID10_DATASUS_MAP[c_code] = c_desc
    except Exception:
        pass


def resolver_cid10_nacional(codigo: str) -> Tuple[str, str]:
    """
    Retorna (descricao_clinica, capitulo_descricao) para qualquer código CID-10 (14.000+ códigos).
    """
    if not codigo:
        return "Diagnóstico Não Informado", "Capítulo Não Identificado"
    
    cod_raw = str(codigo).strip().upper()
    cod_clean = cod_raw.replace(".", "").replace("-", "")
    
    # 1. Busca no dicionario mestre interno (descricoes amigaveis resumidas)
    desc = None
    if cod_raw in CID10_DIAGNOSTICOS_MESTRE:
        desc = CID10_DIAGNOSTICOS_MESTRE[cod_raw]
    elif cod_clean in CID10_DIAGNOSTICOS_MESTRE:
        desc = CID10_DIAGNOSTICOS_MESTRE[cod_clean]
    elif len(cod_clean) >= 3 and cod_clean[:3] in CID10_DIAGNOSTICOS_MESTRE:
        desc = CID10_DIAGNOSTICOS_MESTRE[cod_clean[:3]]
    
    # 2. Busca no catalogo completo oficial do DATASUS (14.257 diagnosticos)
    if not desc:
        if cod_clean in _CID10_DATASUS_MAP:
            desc = _CID10_DATASUS_MAP[cod_clean]
        elif len(cod_clean) >= 3 and cod_clean[:3] in _CID10_DATASUS_MAP:
            desc = _CID10_DATASUS_MAP[cod_clean[:3]]
        else:
            desc = f"CID {cod_raw}"
            
    # Determinar Capítulo por faixa canônica de 3 caracteres
    cat_3 = cod_clean[:3] if len(cod_clean) >= 3 else ""
    cap_nome = "Outras Condições Clínicas"
    
    if cat_3:
        if "A00" <= cat_3 <= "B99":
            cap_nome = "Doenças Infecciosas e Parasitárias"
        elif "C00" <= cat_3 <= "D48":
            cap_nome = "Neoplasias (tumores malignos e benignos)"
        elif "D50" <= cat_3 <= "D89":
            cap_nome = "Doenças do sangue e dos órgãos hematopoéticos"
        elif "E00" <= cat_3 <= "E90":
            cap_nome = "Doenças endócrinas, nutricionais e metabólicas"
        elif "F00" <= cat_3 <= "F99":
            cap_nome = "Transtornos mentais e comportamentais"
        elif "G00" <= cat_3 <= "G99":
            cap_nome = "Doenças do sistema nervoso"
        elif "H00" <= cat_3 <= "H59":
            cap_nome = "Doenças do olho e anexos (Oftalmologia)"
        elif "H60" <= cat_3 <= "H95":
            cap_nome = "Doenças do ouvido e da apófise mastoide (Otorrino)"
        elif "I00" <= cat_3 <= "I99":
            cap_nome = "Doenças do aparelho circulatório (Cardiovasculares / AVC)"
        elif "J00" <= cat_3 <= "J99":
            cap_nome = "Doenças do aparelho respiratório (Pneumonias)"
        elif "K00" <= cat_3 <= "K93":
            cap_nome = "Doenças do aparelho digestivo"
        elif "L00" <= cat_3 <= "L99":
            cap_nome = "Doenças da pele e do tecido subcutâneo"
        elif "M00" <= cat_3 <= "M99":
            cap_nome = "Doenças do sistema osteomuscular e do tecido conjuntivo"
        elif "N00" <= cat_3 <= "N99":
            cap_nome = "Doenças do aparelho geniturinário (Renais)"
        elif "O00" <= cat_3 <= "O99":
            cap_nome = "Gravidez, parto e puerpério (Obstetrícia)"
        elif "P00" <= cat_3 <= "P96":
            cap_nome = "Algumas afecções originadas no período perinatal (Neonatologia)"
        elif "Q00" <= cat_3 <= "Q99":
            cap_nome = "Malformações congênitas, deformidades e anomalias cromossômicas"
        elif "R00" <= cat_3 <= "R99":
            cap_nome = "Sintomas, sinais e achados anormais não classificados"
        elif "S00" <= cat_3 <= "T98":
            cap_nome = "Lesões, envenenamentos e consequências de causas externas (Trauma)"
        elif "V01" <= cat_3 <= "Y98":
            cap_nome = "Causas externas de morbidade e de mortalidade"
        elif "Z00" <= cat_3 <= "Z99":
            cap_nome = "Fatores que influenciam o estado de saúde e contato com serviços"
        elif "U00" <= cat_3 <= "U99":
            cap_nome = "Códigos para propósitos especiais (COVID-19)"

    return desc, cap_nome

