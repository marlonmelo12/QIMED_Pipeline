"""
Predicate Builder - QIMED Lakehouse V3.
Gera predicates SQL para particionamento e operacoes Delta Lake (ex: mode='overwrite')
com introspeccao dinamica de tipos no Schema Arrow (evitando coercao implicita e falhas silenciosas).
"""
from typing import Any, Optional
import pyarrow as pa


def build_partition_predicate(
    schema: pa.Schema,
    year: Any,
    month: Any,
    uf: str,
    subsystem: Optional[str] = None,
) -> str:
    """
    Constroi o predicate SQL para particionamento Delta Lake respeitando estritamente
    os tipos de dados reais presentes no Schema Arrow da tabela ou lote.

    - Colunas string (pa.string(), pa.large_string(), etc.): geram literais com aspas simples ('2026', '05', 'MG').
    - Colunas inteiras (pa.int8(), pa.int16(), pa.int32(), pa.int64(), etc.): geram literais numericos sem aspas (2026, 5).

    Args:
        schema: pa.Schema da tabela Delta ou RecordBatch.
        year: Ano alvo (int ou str).
        month: Mes alvo (int ou str).
        uf: Unidade Federativa (ex: 'MG', 'SP').
        subsystem: Opcional, nome do subsistema ('SIH', 'SIA') caso a tabela seja particionada por subsistema.

    Returns:
        String formatada em SQL com as clausulas unidas por 'AND' (ex: "ano = '2026' AND mes = '05' AND uf = 'MG'").
    """
    clauses = []

    # 1. Clausula 'ano'
    if "ano" in schema.names:
        field_type = schema.field("ano").type
        if pa.types.is_integer(field_type):
            clauses.append(f"ano = {int(year)}")
        else:
            clauses.append(f"ano = '{str(year)}'")

    # 2. Clausula 'mes'
    if "mes" in schema.names:
        field_type = schema.field("mes").type
        if pa.types.is_integer(field_type):
            clauses.append(f"mes = {int(month)}")
        else:
            clauses.append(f"mes = '{int(month):02d}'")

    # 3. Clausula 'uf'
    if "uf" in schema.names:
        clauses.append(f"uf = '{str(uf).upper()}'")

    # 4. Clausula opcional 'subsistema'
    if subsystem and "subsistema" in schema.names:
        clauses.append(f"subsistema = '{str(subsystem).upper()}'")

    return " AND ".join(clauses)
