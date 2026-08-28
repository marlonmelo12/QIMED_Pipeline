"""
Teste do SchemaValidator e Schema Drift - QIMED Lakehouse V3.
"""
import pytest
from src.quality.schema_validator import SchemaValidator


def test_schema_drift_detection():
    expected = ["PA_CODUNI", "PA_PROC_ID", "PA_QTDPRO", "PA_VALPRO"]
    validator = SchemaValidator(expected_columns=expected, table_name="bronze_sia")

    # Cen?rio 1: Schema Exato
    res1 = validator.validate(["PA_CODUNI", "PA_PROC_ID", "PA_QTDPRO", "PA_VALPRO"])
    assert res1["is_valid"] is True
    assert res1["has_drift"] is False

    # Cen?rio 2: Coluna Faltante
    res2 = validator.validate(["PA_CODUNI", "PA_PROC_ID"])
    assert res2["is_valid"] is False
    assert "PA_QTDPRO" in res2["missing_columns"]

    # Cen?rio 3: Coluna Nova (Drift)
    res3 = validator.validate(["PA_CODUNI", "PA_PROC_ID", "PA_QTDPRO", "PA_VALPRO", "NOVA_COLUNA"])
    assert res3["is_valid"] is True
    assert res3["has_drift"] is True
    assert "NOVA_COLUNA" in res3["unexpected_columns"]
