"""
Schema Registry - De-Para Declarativo para a Camada Silver - QIMED Lakehouse.
Mapeia colunas de fontes heterogêneas (Tasy, MV Soul, CSV Genérico/SIH)
para o modelo canônico da tabela fato `fct_internacao`.
"""
from typing import Dict, List, Optional

# Mapeamento do ERP Philips Tasy para o modelo canônico fct_internacao
MAPPING_TASY: Dict[str, str] = {
    "cd_atendimento": "numero_aih",
    "cd_pessoa_fisica": "cpf_paciente",
    "cd_cnes": "codigo_estabelecimento_cnes",
    "cd_estabelecimento": "codigo_estabelecimento_cnes",
    "dt_entrada": "data_internacao",
    "dt_alta": "data_alta",
    "cd_cid_principal": "codigo_cid10_principal",
    "cd_cid_secundario": "codigo_cid10_secundario",
    "vl_total_conta": "valor_total_brl",
    "vl_uti": "valor_uti_brl",
    "ie_sexo": "sexo_biologico",
    "ds_sexo": "sexo_biologico",
    "dt_nascimento": "data_nascimento_paciente",
    "nr_idade": "idade_anos",
    "nr_dias_internacao": "dias_permanencia_real",
    "ie_obito": "indicador_obito",
    "cd_procedimento": "codigo_procedimento_realizado",
    "cd_municipio_paciente": "codigo_municipio_residencia_paciente",
    "cd_municipio_hospital": "codigo_municipio_hospital",
}

# Mapeamento do ERP MV Soul para o modelo canônico fct_internacao
MAPPING_MV_SOUL: Dict[str, str] = {
    "nr_atendimento": "numero_aih",
    "cd_atendimento": "numero_aih",
    "cd_paciente": "cpf_paciente",
    "cd_cnes": "codigo_estabelecimento_cnes",
    "cd_hospital": "codigo_estabelecimento_cnes",
    "cnes": "codigo_estabelecimento_cnes",
    "dt_atendimento": "data_internacao",
    "dt_internacao": "data_internacao",
    "dt_alta_medica": "data_alta",
    "dt_alta": "data_alta",
    "cd_cid": "codigo_cid10_principal",
    "cd_cid_principal": "codigo_cid10_principal",
    "cd_cid_secundario": "codigo_cid10_secundario",
    "vl_total": "valor_total_brl",
    "vl_total_conta": "valor_total_brl",
    "vl_uti": "valor_uti_brl",
    "tp_sexo": "sexo_biologico",
    "dt_nascimento": "data_nascimento_paciente",
    "qt_idade": "idade_anos",
    "qt_dias": "dias_permanencia_real",
    "sn_obito": "indicador_obito",
    "cd_procedimento": "codigo_procedimento_realizado",
    "cd_municipio_res": "codigo_municipio_residencia_paciente",
    "cd_municipio_hosp": "codigo_municipio_hospital",
}

# Mapeamento do layout SIH/DATASUS ou CSV Genérico para o modelo canônico fct_internacao
MAPPING_GENERIC_CSV: Dict[str, str] = {
    "N_AIH": "numero_aih",
    "NUMERO_AIH": "numero_aih",
    "CNES": "codigo_estabelecimento_cnes",
    "CODIGO_CNES": "codigo_estabelecimento_cnes",
    "MUNIC_RES": "codigo_municipio_residencia_paciente",
    "MUNIC_MOV": "codigo_municipio_hospital",
    "NASC": "data_nascimento_paciente",
    "DT_NASC": "data_nascimento_paciente",
    "SEXO": "sexo_biologico",
    "DT_INTER": "data_internacao",
    "DT_SAIDA": "data_alta",
    "DIAG_PRINC": "codigo_cid10_principal",
    "DIAG_SECUN": "codigo_cid10_secundario",
    "DIAS_PERM": "dias_permanencia_real",
    "MORTE": "indicador_obito",
    "VAL_TOT": "valor_total_brl",
    "VAL_UTI": "valor_uti_brl",
    "PROC_REA": "codigo_procedimento_realizado",
    "CPF": "cpf_paciente",
    "IDADE": "idade_anos",
}

CANONICAL_FIELDS: List[str] = [
    "id_atendimento",
    "id_paciente",
    "id_registro",
    "numero_aih",
    "codigo_estabelecimento_cnes",
    "codigo_municipio_residencia_paciente",
    "codigo_municipio_hospital",
    "data_nascimento_paciente",
    "idade_anos",
    "sexo_biologico",
    "data_internacao",
    "data_alta",
    "codigo_cid10_principal",
    "codigo_cid10_secundario",
    "dias_permanencia_real",
    "indicador_obito",
    "valor_total_brl",
    "valor_uti_brl",
    "codigo_procedimento_realizado",
    "cpf_paciente",
]


class SchemaRegistry:
    """
    Registro centralizado de contratos de mapeamento (De-Para) para padronização canônica na Silver.
    """

    def __init__(self):
        self._mappings: Dict[str, Dict[str, str]] = {
            "tasy": MAPPING_TASY,
            "mv_soul": MAPPING_MV_SOUL,
            "generic_csv": MAPPING_GENERIC_CSV,
            "sih": MAPPING_GENERIC_CSV,
        }

    def register_mapping(self, source_name: str, mapping: Dict[str, str]):
        """Registra ou sobrescreve um mapeamento para uma fonte específica."""
        self._mappings[source_name.lower()] = mapping

    def get_mapping(self, source_name: str) -> Dict[str, str]:
        """Recupera o dicionário de De-Para para a fonte solicitada."""
        norm_name = source_name.lower()
        if norm_name not in self._mappings:
            # Fallback para o mapeamento genérico
            return self._mappings.get("generic_csv", {})
        return self._mappings[norm_name]

    def list_sources(self) -> List[str]:
        """Lista todas as fontes com mapeamento configurado."""
        return list(self._mappings.keys())

    def get_canonical_fields(self) -> List[str]:
        """Retorna a lista de campos canônicos esperados na fct_internacao."""
        return CANONICAL_FIELDS.copy()

