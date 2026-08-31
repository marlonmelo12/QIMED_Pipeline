"""
Schema Drift Detector — QIMED Lakehouse V3.

Motor de detecção de drift de schema para fontes DATASUS/ANS.
Executa ANTES da decodificação DBC completa: lê apenas os primeiros N registros
de um RecordBatch Arrow (ou DataFrame) e valida contra o contrato Pydantic.

Princípios:
  - Fail-fast: campos REQUIRED ausentes → SchemaContractViolation imediato.
  - Warning: campos EXPECTED ausentes → registrados, não bloqueiam.
  - Novos campos desconhecidos → ignorados (schema evolution aceitável).
  - Sem efeitos colaterais: nunca grava em disco, não conecta a fontes externas.
  - Thread-safe: sem estado mutável compartilhado.

Integração no pipeline:
    StagingWriter.process_batch_stream() chama DriftDetector.validate_batch()
    no PRIMEIRO batch recebido de cada subsistema. Se a validação falhar com
    nível REQUIRED, um SchemaContractViolation é lançado e o processamento da
    UF/competência para imediatamente — o erro é registrado no manifesto como
    "schema_drift_detected" pelo orquestrador.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa

from src.quality.schema_contracts import (
    FieldSeverity,
    SubsystemContract,
    get_contract,
)

logger = logging.getLogger("QIMED_DRIFT")


# ─────────────────────────────────────────────────────────────────────────────
# Resultado da validação
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FieldDriftReport:
    """Anomalia encontrada em um campo individual."""
    field_name: str
    severity: FieldSeverity
    issue: str  # "missing", "type_mismatch", "all_null"
    detail: str = ""
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class SchemaDriftReport:
    """Relatório completo de uma validação de schema."""
    subsystem: str
    competencia: str          # ex.: "2026/05" para contexto de log
    uf: str
    probe_rows: int           # quantas linhas foram inspecionadas
    columns_found: Set[str]   # colunas presentes na fonte
    columns_expected: Set[str]
    columns_missing_required: Set[str]
    columns_missing_expected: Set[str]
    field_reports: List[FieldDriftReport] = field(default_factory=list)

    @property
    def has_critical_violations(self) -> bool:
        return len(self.columns_missing_required) > 0 or any(
            r.severity == FieldSeverity.REQUIRED for r in self.field_reports
        )

    @property
    def has_warnings(self) -> bool:
        return len(self.columns_missing_expected) > 0 or any(
            r.severity == FieldSeverity.EXPECTED for r in self.field_reports
        )

    def summary(self) -> str:
        lines = [
            f"[SCHEMA DRIFT] {self.subsystem} {self.uf} {self.competencia} — {self.probe_rows} linhas inspecionadas",
            f"  Colunas na fonte: {len(self.columns_found)}",
            f"  Colunas esperadas pelo contrato: {len(self.columns_expected)}",
        ]
        if self.columns_missing_required:
            lines.append(f"  ❌ REQUIRED AUSENTES: {sorted(self.columns_missing_required)}")
        if self.columns_missing_expected:
            lines.append(f"  ⚠️  EXPECTED AUSENTES: {sorted(self.columns_missing_expected)}")
        for r in self.field_reports:
            icon = "❌" if r.severity == FieldSeverity.REQUIRED else "⚠️ "
            lines.append(f"  {icon} {r.field_name}: {r.issue} — {r.detail}")
        if not self.has_critical_violations and not self.has_warnings:
            lines.append("  ✓ Schema compatível com o contrato.")
        return "\n".join(lines)


class SchemaContractViolation(Exception):
    """
    Lançada quando um campo REQUIRED está ausente ou com tipo incompatível.
    Contém o SchemaDriftReport completo para logging estruturado.
    """
    def __init__(self, report: SchemaDriftReport):
        self.report = report
        super().__init__(report.summary())


# ─────────────────────────────────────────────────────────────────────────────
# Motor de validação
# ─────────────────────────────────────────────────────────────────────────────

class SchemaDriftDetector:
    """
    Valida um Arrow RecordBatch (ou lista de registros) contra o contrato Pydantic
    do subsistema, lendo apenas os primeiros `probe_rows` registros.

    Parâmetros
    ----------
    probe_rows : int
        Quantas linhas inspecionar. Padrão 200 — suficiente para detectar drift
        sem impacto de performance no pipeline principal.
    strict_mode : bool
        Se True (padrão), lança SchemaContractViolation em violações REQUIRED.
        Se False, registra o erro mas não interrompe o fluxo (útil para backfill).
    """

    def __init__(self, probe_rows: int = 200, strict_mode: bool = True):
        self.probe_rows = probe_rows
        self.strict_mode = strict_mode

    # ── API pública ──────────────────────────────────────────────────────────

    def validate_batch(
        self,
        batch: pa.RecordBatch,
        subsystem: str,
        uf: str = "??",
        year: int = 0,
        month: int = 0,
    ) -> SchemaDriftReport:
        """
        Valida o primeiro `probe_rows` de um Arrow RecordBatch.

        Em violações REQUIRED + strict_mode=True → lança SchemaContractViolation.
        Sempre retorna SchemaDriftReport (mesmo em violação) para logging.
        """
        contract = get_contract(subsystem)
        if contract is None:
            logger.debug(
                f"[DRIFT SKIP] Sem contrato registrado para subsistema '{subsystem}'. "
                f"Validação ignorada."
            )
            # Retorna relatório vazio: sem contrato, sem erro
            return SchemaDriftReport(
                subsystem=subsystem,
                competencia=f"{year}/{month:02d}",
                uf=uf,
                probe_rows=0,
                columns_found=set(batch.schema.names),
                columns_expected=set(),
                columns_missing_required=set(),
                columns_missing_expected=set(),
            )

        # Toma amostra
        probe_batch = batch.slice(0, min(self.probe_rows, len(batch)))
        report = self._validate_against_contract(probe_batch, contract, uf, year, month)

        # Log estruturado
        if report.has_critical_violations:
            logger.error(report.summary())
            if self.strict_mode:
                raise SchemaContractViolation(report)
        elif report.has_warnings:
            logger.warning(report.summary())
        else:
            logger.info(report.summary())

        return report

    def validate_records(
        self,
        records: List[Dict[str, Any]],
        subsystem: str,
        uf: str = "??",
        year: int = 0,
        month: int = 0,
    ) -> SchemaDriftReport:
        """
        Valida uma lista de dicionários (saída do DBFRead).
        Converte para Arrow internamente antes de delegar a validate_batch.
        """
        import pandas as pd
        probe = records[:self.probe_rows]
        if not probe:
            raise ValueError(f"Lista de registros vazia — impossível validar schema de {subsystem}.")
        df = pd.DataFrame(probe)
        df.columns = [str(c).upper().strip() for c in df.columns]
        batch = pa.RecordBatch.from_pandas(df, preserve_index=False)
        return self.validate_batch(batch, subsystem, uf, year, month)

    # ── Validação interna ────────────────────────────────────────────────────

    def _validate_against_contract(
        self,
        batch: pa.RecordBatch,
        contract: SubsystemContract,
        uf: str,
        year: int,
        month: int,
    ) -> SchemaDriftReport:
        cols_in_source = {c.upper() for c in batch.schema.names}
        cols_expected  = {c.upper() for c in contract.all_field_names()}

        missing_required: Set[str] = set()
        missing_expected: Set[str] = set()
        field_reports: List[FieldDriftReport] = []

        for fc in contract.fields:
            col_upper = fc.name.upper()

            # ── Verificação de presença ──────────────────────────────────────
            if col_upper not in cols_in_source:
                if fc.severity == FieldSeverity.REQUIRED:
                    missing_required.add(col_upper)
                    field_reports.append(FieldDriftReport(
                        field_name=col_upper,
                        severity=FieldSeverity.REQUIRED,
                        issue="missing",
                        detail=f"Campo obrigatório ausente. {fc.description}",
                    ))
                else:
                    missing_expected.add(col_upper)
                    field_reports.append(FieldDriftReport(
                        field_name=col_upper,
                        severity=FieldSeverity.EXPECTED,
                        issue="missing",
                        detail=f"Campo esperado ausente (não bloqueante). {fc.description}",
                    ))
                continue

            # ── Verificação de tipo em campos REQUIRED ───────────────────────
            if fc.severity == FieldSeverity.REQUIRED:
                col_idx = batch.schema.get_field_index(
                    next(c for c in batch.schema.names if c.upper() == col_upper)
                )
                col_array = batch.column(col_idx)

                # Verifica se todas as linhas são nulas (coluna presente mas vazia)
                if col_array.null_count == len(col_array):
                    field_reports.append(FieldDriftReport(
                        field_name=col_upper,
                        severity=FieldSeverity.REQUIRED,
                        issue="all_null",
                        detail=(
                            f"Coluna obrigatória presente mas 100% nula em {self.probe_rows} registros. "
                            f"Possível mudança de layout ou truncamento de dado."
                        ),
                    ))
                    missing_required.add(col_upper)
                    continue

                # Verificação de tipo: pega primeiros valores não-nulos
                sample_vals = []
                type_ok = True
                for val in col_array.to_pylist():
                    if val is None:
                        continue
                    sample_vals.append(val)
                    if not fc.accepts(val):
                        type_ok = False
                    if len(sample_vals) >= 5:
                        break

                if not type_ok:
                    field_reports.append(FieldDriftReport(
                        field_name=col_upper,
                        severity=FieldSeverity.REQUIRED,
                        issue="type_mismatch",
                        detail=(
                            f"Tipo incompatível. Esperado: {fc.accepted_types}. "
                            f"Tipo Arrow: {col_array.type}. "
                            f"Exemplos: {sample_vals[:3]}"
                        ),
                        sample_values=sample_vals[:3],
                    ))
                    missing_required.add(col_upper)

        return SchemaDriftReport(
            subsystem=contract.subsystem,
            competencia=f"{year}/{month:02d}",
            uf=uf,
            probe_rows=len(batch),
            columns_found=cols_in_source,
            columns_expected=cols_expected,
            columns_missing_required=missing_required,
            columns_missing_expected=missing_expected,
            field_reports=field_reports,
        )
