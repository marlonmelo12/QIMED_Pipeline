"""
Data Quality Auditor & Clinical Inconsistency Checker - QIMED Lakehouse V3.
Executa auditoria quantitativa (reconcilia??o de linhas) e cl?nica
com severidades (INFO, WARNING, ERROR) sobre o DuckDB DW.
"""
import os
import duckdb
from typing import Any, Dict, List, Optional
import pandas as pd

from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class DataQualityAuditor:
    """
    Auditor forense de qualidade de dados de sa?de.
    """

    def __init__(self, dw_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        cfg = config or load_pipeline_config()
        self.dw_path = dw_path or cfg.get("paths", {}).get("gold_dw_file", "warehouse/qimed_dw.duckdb")

    def audit_full_warehouse(self) -> Dict[str, Any]:
        """
        Executa bateria forense de testes cl?nicos e financeiros.
        """
        if not os.path.exists(self.dw_path):
            return {"status": "error", "message": "Arquivo DW DuckDB nao encontrado."}

        conn = duckdb.connect(self.dw_path, read_only=True)
        findings = []

        try:
            tables = [r[0] for r in conn.execute("SHOW TABLES;").fetchall()]

            # 1. Auditoria de Contagem de Fatos
            for t in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                findings.append({
                    "check": "Volumetria da Tabela",
                    "tabela": t,
                    "resultado": f"{count:,} registros",
                    "severidade": "INFO",
                })

            # 2. Inconsist?ncia de Valores Negativos
            if "agg_internacoes_uf" in tables:
                neg = conn.execute("SELECT COUNT(*) FROM agg_internacoes_uf WHERE valor_total_brl < 0").fetchone()[0]
                findings.append({
                    "check": "Valores Financeiros Negativos em Interna??es",
                    "tabela": "agg_internacoes_uf",
                    "resultado": f"{neg} registros com valor negativo",
                    "severidade": "ERROR" if neg > 0 else "INFO",
                })

            # 3. Taxa de Mortalidade > 50%
            if "agg_internacoes_uf" in tables:
                high_mort = conn.execute("SELECT uf, taxa_mortalidade_pct FROM agg_internacoes_uf WHERE taxa_mortalidade_pct > 50").fetchall()
                if high_mort:
                    findings.append({
                        "check": "Alerta de Mortalidade Elevada (>50%)",
                        "tabela": "agg_internacoes_uf",
                        "resultado": f"UFs detectadas: {high_mort}",
                        "severidade": "WARNING",
                    })

            return {"status": "completed", "findings": findings, "total_checks": len(findings)}
        finally:
            conn.close()
