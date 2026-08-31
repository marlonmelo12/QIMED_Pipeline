"""
Contratos de Schema Pydantic — QIMED Lakehouse V3.

Define os contratos mínimos de schema para cada subsistema DATASUS/ANS.
Um "contrato mínimo" não exige que TODOS os campos existam — apenas que os
campos críticos para as PKs, MPI e transformações Silver estejam presentes
e com tipo compatível com a transformação downstream.

Filosofia de drift:
  - Campos OBRIGATÓRIOS (required): ausência → falha imediata (fail-fast).
  - Campos ESPERADOS  (expected):  ausência → warning + log de anomalia.
  - Tipo incompatível em campo obrigatório → falha imediata.
  - Novos campos desconhecidos → ignorados (schema evolution aceitável).

Compatibilidade: Pydantic v2 (model_validator, model_config).
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class FieldSeverity(str, Enum):
    REQUIRED = "required"   # Ausência → fail-fast
    EXPECTED = "expected"   # Ausência → warning, não bloqueia


class FieldContract(BaseModel):
    """Contrato de um campo individual."""
    name: str
    severity: FieldSeverity = FieldSeverity.REQUIRED
    # Tipos Python aceitos como string lowercase: "str", "int", "float", "bool"
    accepted_types: List[str] = Field(default_factory=lambda: ["str"])
    description: str = ""

    def accepts(self, value: Any) -> bool:
        """Verifica se `value` é compatível com os tipos aceitos pelo campo."""
        type_map = {
            "str": str,
            "int": int,
            "float": (int, float),
            "bool": bool,
        }
        for t in self.accepted_types:
            target = type_map.get(t)
            if target and isinstance(value, target):
                return True
        # Aceita None/NaN explicitamente (tipo pode ter chegado como None)
        if value is None:
            return True
        # Tenta coerção implícita de string numérica
        if "int" in self.accepted_types or "float" in self.accepted_types:
            try:
                float(str(value))
                return True
            except (ValueError, TypeError):
                pass
        return False


class SubsystemContract(BaseModel):
    """Contrato completo de um subsistema (conjunto de campos)."""
    subsystem: str
    version: str = "v3"
    description: str = ""
    fields: List[FieldContract]

    def required_field_names(self) -> Set[str]:
        return {f.name for f in self.fields if f.severity == FieldSeverity.REQUIRED}

    def expected_field_names(self) -> Set[str]:
        return {f.name for f in self.fields if f.severity == FieldSeverity.EXPECTED}

    def all_field_names(self) -> Set[str]:
        return {f.name for f in self.fields}

    def get_field(self, name: str) -> Optional[FieldContract]:
        for f in self.fields:
            if f.name == name:
                return f
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Contratos por subsistema
# ─────────────────────────────────────────────────────────────────────────────

#: SIH — AIH Reduzida (RD). Fonte: SIHSUS/200801_/Dados/RD{UF}{AAMM}.dbc
SIH_RD_CONTRACT = SubsystemContract(
    subsystem="SIH",
    description="AIH Reduzida — campos obrigatorios para PK sha256, MPI e Star Schema Silver",
    fields=[
        # ── PKs e identificadores (obrigatórios para surrogate key determinística)
        FieldContract(name="N_AIH",    severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Número da AIH (13 dígitos). Parte da PK sha256."),
        FieldContract(name="IDENT",    severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Tipo da AIH (1=principal, 5=subsequente). Discrimina PKs em retornar."),
        FieldContract(name="CNES",     severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Código CNES do estabelecimento. Parte da PK e FK para dim_estabelecimento."),
        FieldContract(name="PROC_REA", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Código SIGTAP do procedimento realizado. Parte da PK sha256."),
        FieldContract(name="DT_SAIDA", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Data de alta (AAAAMMDD). Parte da PK sha256."),
        FieldContract(name="DT_INTER", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Data de internação (AAAAMMDD). Usada no cálculo de permanência e MPI."),
        # ── MPI / Pseudonimização
        FieldContract(name="NASC",      severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Data de nascimento (AAAAMMDD). Chave MPI nível 3/4."),
        FieldContract(name="SEXO",      severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Sexo biológico. Harmonizado em M/F/I na Silver."),
        FieldContract(name="MUNIC_RES", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Código IBGE do município de residência. Chave MPI e FK territorial."),
        # ── Diagnósticos clínicos
        FieldContract(name="DIAG_PRINC", severity=FieldSeverity.REQUIRED, accepted_types=["str"],
                      description="CID-10 principal. Usado em regras de guardrail e KPIs clínicos."),
        FieldContract(name="DIAG_SECUN", severity=FieldSeverity.EXPECTED,  accepted_types=["str"],
                      description="CID-10 secundário. Sujeito a sanitização de sentinelas."),
        # ── Financeiro
        FieldContract(name="VAL_TOT",  severity=FieldSeverity.REQUIRED, accepted_types=["str", "int", "float"],
                      description="Valor total da AIH em BRL. Usado em KPIs financeiros e target_alto_custo."),
        FieldContract(name="VAL_UTI",  severity=FieldSeverity.EXPECTED,  accepted_types=["str", "int", "float"],
                      description="Valor de UTI. Pode estar ausente em AIHs sem UTI."),
        FieldContract(name="MORTE",    severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Indicador de óbito (1=sim). Target ML e KPI de mortalidade."),
        FieldContract(name="DIAS_PERM", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Dias de permanência faturados. Target ML longa permanência."),
        FieldContract(name="MUNIC_MOV", severity=FieldSeverity.EXPECTED,  accepted_types=["str", "int"],
                      description="Código IBGE do município do hospital (movimento)."),
    ],
)

#: SIA — Produção Ambulatorial (PA). Fonte: SIASUS/200801_/Dados/PA{UF}{AAMM}.dbc
SIA_PA_CONTRACT = SubsystemContract(
    subsystem="SIA",
    description="Produção Ambulatorial — campos obrigatorios para PK sha256, SIA Silver e flags contábeis",
    fields=[
        # ── PKs (7 campos da surrogate key determinística)
        FieldContract(name="PA_CODUNI",  severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="CNES do estabelecimento. Parte da PK sha256."),
        FieldContract(name="PA_PROC_ID", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Código SIGTAP do procedimento. Parte da PK sha256."),
        FieldContract(name="PA_CMP",     severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Competência (AAAAMM). Parte da PK sha256."),
        FieldContract(name="PA_MUNPCN",  severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Município de residência do paciente. Parte da PK e FK territorial."),
        FieldContract(name="PA_CNS_PAC", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="CNS do paciente (anonimizado na fonte). Parte da PK sha256."),
        FieldContract(name="PA_SEXO",    severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Sexo biológico. Harmonizado em M/F/I na Silver."),
        FieldContract(name="PA_IDADE",   severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Idade do paciente. Parte da PK sha256."),
        # ── Diagnósticos
        FieldContract(name="PA_CIDPRI", severity=FieldSeverity.REQUIRED, accepted_types=["str"],
                      description="CID-10 principal do atendimento ambulatorial."),
        FieldContract(name="PA_CIDSEC", severity=FieldSeverity.EXPECTED,  accepted_types=["str"],
                      description="CID-10 secundário."),
        # ── Quantidade/Valor (flags contábeis V2-11)
        FieldContract(name="PA_QTDPRO", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int", "float"],
                      description="Quantidade produzida. Base para flag_glosa_sia."),
        FieldContract(name="PA_QTDAPR", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int", "float"],
                      description="Quantidade aprovada. Base para flag_glosa_sia e flag_pab_tarifa_zero."),
        FieldContract(name="PA_VALPRO", severity=FieldSeverity.EXPECTED,  accepted_types=["str", "int", "float"],
                      description="Valor produzido em BRL."),
        FieldContract(name="PA_VALAPR", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int", "float"],
                      description="Valor aprovado em BRL. Base para flag_pab_tarifa_zero."),
        FieldContract(name="PA_GESTAO", severity=FieldSeverity.EXPECTED,  accepted_types=["str", "int"],
                      description="Código de gestão (UF/Município)."),
        FieldContract(name="PA_UFMUN",  severity=FieldSeverity.EXPECTED,  accepted_types=["str", "int"],
                      description="UF+Município do estabelecimento."),
    ],
)

#: ANS — Ressarcimento ao SUS (RJ). Fonte: ANS FTP/API.
ANS_RESSARCIMENTO_CONTRACT = SubsystemContract(
    subsystem="ANS_RESSARCIMENTO",
    description="Ressarcimento ANS ao SUS — campos obrigatórios para PK, bridge MPI e flags contábeis",
    fields=[
        FieldContract(name="identificador_cobranca_abi", severity=FieldSeverity.REQUIRED,
                      accepted_types=["str", "int"],
                      description="Identificador único da cobrança ABI. PK da fct_ressarcimento_sus."),
        FieldContract(name="numero_aih",  severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Número da AIH. Bridge com fct_internacao para herança do MPI."),
        FieldContract(name="codigo_registro_ans", severity=FieldSeverity.REQUIRED,
                      accepted_types=["str", "int"],
                      description="Registro ANS da operadora. FK para dim_operadoras_saude."),
        FieldContract(name="situacao_cobranca", severity=FieldSeverity.REQUIRED, accepted_types=["str"],
                      description="Status da cobrança (PAGO, IMPUGNADO, EM RECURSO). Base para flag contábil."),
        FieldContract(name="valor_notificado_brl", severity=FieldSeverity.REQUIRED,
                      accepted_types=["str", "int", "float"],
                      description="Valor notificado em BRL."),
        FieldContract(name="valor_recolhido_brl", severity=FieldSeverity.EXPECTED,
                      accepted_types=["str", "int", "float"],
                      description="Valor recolhido em BRL. Pode ser 0 ou ausente para impugnados."),
        FieldContract(name="data_internacao", severity=FieldSeverity.EXPECTED, accepted_types=["str", "int"],
                      description="Data de internação (AAAAMM ou AAAAMMDD)."),
        FieldContract(name="uf", severity=FieldSeverity.REQUIRED, accepted_types=["str"],
                      description="UF de origem do registro. Coluna de partição Delta."),
    ],
)

#: SIH-RJ — AIHs Rejeitadas.
SIH_RJ_CONTRACT = SubsystemContract(
    subsystem="SIH_RJ",
    description="AIHs Rejeitadas — campos obrigatorios para PK de glosas hospitalares",
    fields=[
        FieldContract(name="N_AIH",    severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Número da AIH rejeitada. Parte da PK sha256 de glosa."),
        FieldContract(name="CNES",     severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="CNES do estabelecimento. Parte da PK sha256."),
        FieldContract(name="PROC_REA", severity=FieldSeverity.REQUIRED, accepted_types=["str", "int"],
                      description="Procedimento rejeitado. Parte da PK sha256."),
        FieldContract(name="VAL_TOT",  severity=FieldSeverity.REQUIRED, accepted_types=["str", "int", "float"],
                      description="Valor glosado em BRL."),
        FieldContract(name="MUNIC_MOV", severity=FieldSeverity.EXPECTED, accepted_types=["str", "int"],
                      description="Município do hospital."),
    ],
)


# ─────────────────────────────────────────────────────────────────────────────
# Registro central
# ─────────────────────────────────────────────────────────────────────────────

#: Mapeamento normalizado: chave → contrato.
CONTRACTS: Dict[str, SubsystemContract] = {
    "SIH":               SIH_RD_CONTRACT,
    "SIH_RD":            SIH_RD_CONTRACT,
    "SIH-RD":            SIH_RD_CONTRACT,
    "SIH_RJ":            SIH_RJ_CONTRACT,
    "SIH-RJ":            SIH_RJ_CONTRACT,
    "SIA":               SIA_PA_CONTRACT,
    "ANS_RESSARCIMENTO": ANS_RESSARCIMENTO_CONTRACT,
}


def get_contract(subsystem: str) -> Optional[SubsystemContract]:
    """Retorna o contrato do subsistema ou None se não houver contrato definido."""
    return CONTRACTS.get(subsystem.upper().replace("-", "_"))
