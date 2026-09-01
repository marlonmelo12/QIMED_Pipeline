"""
Módulo de Detecção Estatística de Outliers e Anomalias Setoriais - QIMED Analytics.

Implementa detecção robusta de outliers utilizando o Modified Z-Score baseado em MAD
(Median Absolute Deviation - Iglewicz & Hoaglin, 1993) e análise de dominância setorial.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import numpy as np


@dataclass
class OutlierItem:
    """Representa uma entidade identificada como outlier estatístico."""
    id_entidade: str
    nome: str
    valor: float
    modified_z_score: float
    concentracao_pct: float
    detalhes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OutlierDetectionResult:
    """Resultado estruturado da análise de outliers setoriais."""
    has_outlier: bool
    metodologia: str
    threshold_mad: float
    threshold_concentracao_pct: float
    total_entidades_analisadas: int
    outliers_count: int
    operadora_outlier_principal: Optional[str] = None
    codigo_outlier_principal: Optional[str] = None
    concentracao_pct_principal: Optional[float] = None
    outliers_detectados: List[OutlierItem] = field(default_factory=list)
    mensagem: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Converte o resultado em dicionário para contratos de API REST."""
        return {
            "has_outlier": self.has_outlier,
            "metodologia": self.metodologia,
            "threshold_mad": self.threshold_mad,
            "threshold_concentracao_pct": self.threshold_concentracao_pct,
            "total_entidades_analisadas": self.total_entidades_analisadas,
            "outliers_count": self.outliers_count,
            "operadora_outlier": self.operadora_outlier_principal,
            "codigo_registro_ans": self.codigo_outlier_principal,
            "concentracao_pct": self.concentracao_pct_principal,
            "outliers_detectados": [
                {
                    "codigo_registro_ans": item.id_entidade,
                    "razao_social": item.nome,
                    "valor_glosado_brl": round(item.valor, 2),
                    "modified_z_score": round(item.modified_z_score, 2),
                    "concentracao_pct": round(item.concentracao_pct, 2),
                }
                for item in self.outliers_detectados
            ],
            "mensagem": self.mensagem,
        }


def detectar_outliers_mad(
    entidades: List[Dict[str, Any]],
    campo_valor: str = "valor_total_glosado_brl",
    campo_id: str = "codigo_registro_ans",
    campo_nome: str = "razao_social",
    threshold_mad: float = 3.5,
    threshold_concentracao_pct: float = 50.0,
) -> OutlierDetectionResult:
    """
    Executa a detecção de anomalias utilizando Modified Z-Score via Median Absolute Deviation (MAD).

    Justificativa Estatística:
    Em dados de saúde e faturamento hospitalar, a distribuição de glosas possui caudas pesadas (heavy-tailed)
    e assimetria acentuada. O desvio-padrão clássico (Z-Score) é inflado pela própria anomalia, gerando
    falsos negativos. O MAD (Median Absolute Deviation) utiliza a mediana como estimador de posição central
    e a mediana dos desvios absolutos como estimador de dispersão, sendo imune a distorções causadas por outliers.

    Fórmula:
        M_i = 0.6745 * |x_i - mediana(X)| / MAD(X)
        onde MAD(X) = mediana(|X - mediana(X)|)

    Critério de Classificação:
        1. Modified Z-score >= threshold_mad (padrão 3.5 sugerido por Iglewicz & Hoaglin)
        OU
        2. Concentração individual >= threshold_concentracao_pct (ex: concentração >= 50% do total do setor).
    """
    total_entidades = len(entidades)
    if total_entidades == 0:
        return OutlierDetectionResult(
            has_outlier=False,
            metodologia="modified_z_score_mad",
            threshold_mad=threshold_mad,
            threshold_concentracao_pct=threshold_concentracao_pct,
            total_entidades_analisadas=0,
            outliers_count=0,
            mensagem="Nenhuma entidade fornecida para análise de anomalias.",
        )

    valores = np.array([float(e.get(campo_valor) or 0.0) for e in entidades], dtype=np.float64)
    soma_total = float(np.sum(valores))

    if soma_total == 0.0 or total_entidades < 3:
        return OutlierDetectionResult(
            has_outlier=False,
            metodologia="modified_z_score_mad",
            threshold_mad=threshold_mad,
            threshold_concentracao_pct=threshold_concentracao_pct,
            total_entidades_analisadas=total_entidades,
            outliers_count=0,
            mensagem="Volume amostral ou financeiro insuficiente para caracterização de outlier.",
        )

    mediana = float(np.median(valores))
    desvios_absolutos = np.abs(valores - mediana)
    mad = float(np.median(desvios_absolutos))

    outliers_encontrados: List[OutlierItem] = []

    for i, e in enumerate(entidades):
        val = valores[i]
        conc_pct = (val / soma_total * 100.0) if soma_total > 0 else 0.0

        if mad > 0:
            m_score = float(0.6745 * (val - mediana) / mad)
        else:
            m_score = float((val - mediana) / (np.mean(desvios_absolutos) + 1e-9)) if (val - mediana) > 0 else 0.0

        is_outlier = (m_score >= threshold_mad and val > mediana) or (conc_pct >= threshold_concentracao_pct)

        if is_outlier:
            outliers_encontrados.append(
                OutlierItem(
                    id_entidade=str(e.get(campo_id, "")),
                    nome=str(e.get(campo_nome, "OPERADORA")),
                    valor=val,
                    modified_z_score=m_score,
                    concentracao_pct=conc_pct,
                    detalhes=e,
                )
            )

    outliers_encontrados.sort(key=lambda x: x.valor, reverse=True)
    outliers_count = len(outliers_encontrados)

    if outliers_count == 0:
        return OutlierDetectionResult(
            has_outlier=False,
            metodologia="modified_z_score_mad",
            threshold_mad=threshold_mad,
            threshold_concentracao_pct=threshold_concentracao_pct,
            total_entidades_analisadas=total_entidades,
            outliers_count=0,
            mensagem="Nenhuma concentração ou desvio atípico de glosas detectado no setor.",
        )

    principal = outliers_encontrados[0]

    if outliers_count == 1:
        msg = (
            f"Operadora '{principal.nome}' identificada como outlier estatístico isolado "
            f"(Modified Z-Score: {principal.modified_z_score:.2f}, Concentração: {principal.concentracao_pct:.2f}%). "
            f"Média setorial recalculada com expurgo da operadora."
        )
    else:
        msg = (
            f"{outliers_count} operadoras identificadas como outliers estatísticos setoriais "
            f"(Operadora principal: '{principal.nome}' com {principal.concentracao_pct:.2f}% de concentração). "
            f"Média setorial recalculada com expurgo."
        )

    return OutlierDetectionResult(
        has_outlier=True,
        metodologia="modified_z_score_mad",
        threshold_mad=threshold_mad,
        threshold_concentracao_pct=threshold_concentracao_pct,
        total_entidades_analisadas=total_entidades,
        outliers_count=outliers_count,
        operadora_outlier_principal=principal.nome,
        codigo_outlier_principal=principal.id_entidade,
        concentracao_pct_principal=round(principal.concentracao_pct, 2),
        outliers_detectados=outliers_encontrados,
        mensagem=msg,
    )
