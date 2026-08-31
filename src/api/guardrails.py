"""
Middleware de Guardrails Clínicos na API - QIMED Lakehouse V3.
Regras de pós-processamento para anular inferências biologicamente impossíveis
decorrentes de erros de digitação hospitalar (taxa de 0,03% no DATASUS).
"""
from typing import Dict, Any

# Prefixos de CIDs estritamente femininos (Obstetrícia, Ginecologia, Aparelho Reprodutor Feminino)
_FEMALE_ONLY_PREFIXES = tuple(
    ["O"] +
    [f"C{i}" for i in range(51, 59)] +
    [f"N{i}" for i in range(70, 99)] +
    ["Z34", "Z35", "Z36"]
)

# Prefixos de CIDs estritamente masculinos (Próstata, Pênis, Testículo, Aparelho Reprodutor Masculino)
_MALE_ONLY_PREFIXES = tuple(
    [f"C{i}" for i in range(60, 64)] +
    [f"N{i}" for i in range(40, 52)]
)


class ClinicalInferenceGuardrails:
    """
    Regras de pós-processamento para anular inferências biologicamente impossíveis
    decorrentes de erros de digitação hospitalar (taxa de 0,03% no DATASUS).
    """
    
    @staticmethod
    def sanitizar_predicao(payload_entrada: Dict[str, Any], predicao_bruta: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitiza predições brutas da API aplicando guardrails de consistência biológica.
        """
        sexo = str(payload_entrada.get("sexo_biologico", "")).strip().upper()

        # [V2-04] Verificar CID antes de str() para evitar que None -> "NONE"
        # passe silenciosamente pelo guardrail sem acionar nenhum alerta.
        cid_raw = payload_entrada.get("codigo_cid10_principal")
        predicao_ajustada = predicao_bruta.copy()

        _CID_VAZIOS = {"", "NONE", "NAN", "<NA>", "N/A", "NA", "0000", "NULL"}
        if cid_raw is None or str(cid_raw).strip().upper() in _CID_VAZIOS:
            predicao_ajustada["alerta_clinico"] = "CID_AUSENTE_GUARDRAIL_INAPLICAVEL"
            predicao_ajustada["confiabilidade_predicao"] = "BAIXA_SEM_DIAGNOSTICO"
            return predicao_ajustada

        cid = str(cid_raw).strip().upper()

        # Regra 1: Homens com CIDs puramente obstétricos/ginecológicos
        if sexo == "M":
            if cid.startswith(_FEMALE_ONLY_PREFIXES):
                predicao_ajustada["alerta_clinico"] = "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
                predicao_ajustada["risco_obstetrico_ajustado"] = 0.0
                predicao_ajustada["confiabilidade_predicao"] = "BAIXA_ERRO_CADASTRO"

        # Regra 2: Mulheres com CIDs puramente prostáticos/masculinos
        elif sexo == "F":
            if cid.startswith(_MALE_ONLY_PREFIXES):
                predicao_ajustada["alerta_clinico"] = "INCONSISTENCIA_BIOLOGICA_SEXO_CID"
                predicao_ajustada["risco_urologico_masculino_ajustado"] = 0.0
                predicao_ajustada["confiabilidade_predicao"] = "BAIXA_ERRO_CADASTRO"

        # Regra 3: Sexo Ignorado / Indeterminado
        elif sexo in ("I", "IGNORADO", "INDETERMINADO", "UNKNOWN", "U"):
            predicao_ajustada["alerta_clinico"] = "SEXO_INDETERMINADO_INFERENCIA_RESTRITA"
            predicao_ajustada["confiabilidade_predicao"] = "MEDIA_SEXO_IGNORADO"

        return predicao_ajustada
