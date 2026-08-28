"""
Teste de conformidade de Nomenclatura em Portugu?s - QIMED Lakehouse V3.
"""
import pytest
from src.lakehouse.silver_writer import SilverWriter
from src.metadata.catalogo_dados import CATALOGO_ENTIDADES


def test_silver_naming_portuguese():
    forbidden_terms = ["patients", "encounters", "conditions", "procedures", "health_plans", "organizations"]
    for table_name in SilverWriter.SILVER_TABLES:
        for term in forbidden_terms:
            assert term not in table_name, f"Tabela Silver {table_name} contem termo proibido em ingles: {term}"


def test_catalogo_entidades_portuguese():
    for table_name in CATALOGO_ENTIDADES.keys():
        assert "_" in table_name or table_name.startswith("lakehouse"), f"Nome invalido: {table_name}"
