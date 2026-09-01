"""
Router de Drill-Down Analítico - QIMED Health Platform (BFF Pattern).
Fornece dados aprofundados para as telas de detalhe de KPIs em uma única requisição HTTP.
"""
from typing import Any, Dict, Optional, Union
from fastapi import APIRouter, Query, Request, Response
from src.api.cache import (
    cached_drilldown_ticket_medio,
    cached_drilldown_custo_total,
    cached_drilldown_custo_desfecho,
)

router = APIRouter(prefix="/analytics/drilldown", tags=["Analytics - Drilldown"])


def _sanitize_periodo(periodo: str) -> str:
    """Remove aspas simples para prevenção de SQL injection. Ex: '2026-05' → '2026-05'."""
    return periodo.replace("'", "").strip()


def _sanitize_uf(uf: str) -> str:
    """Normaliza UF para 2 chars maiúsculos sem aspas. Ex: 'ce' → 'CE'."""
    return uf.upper().replace("'", "").strip()[:2]


@router.get("/ticket-medio")
async def get_drilldown_ticket_medio(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: Optional[str] = Query(None, description="Sigla da UF opcional com 2 caracteres (ex: 'CE')"),
    limit: int = Query(50, ge=1, le=500, description="Limite de registros para ranking hospitalar"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
) -> Any:
    """
    Retorna o detalhamento aprofundado do Ticket Médio Hospitalar:
    - KPIs Estatísticos (Média, Mediana, P75, Máx, Mín, Desvio Padrão);
    - Curva de Percentis (P25, P50, P75, P90, P99);
    - Evolução Mensal dos últimos 6 meses;
    - Ranking de Hospitais CNES com paginação;
    - Quebra por UF.
    """
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf) if uf else ""
    return cached_drilldown_ticket_medio(periodo=p, uf=u, limit=limit, offset=offset, request=request)


@router.get("/custo-total")
async def get_drilldown_custo_total(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: Optional[str] = Query(None, description="Sigla da UF opcional com 2 caracteres (ex: 'CE')"),
    limit: int = Query(50, ge=1, le=500, description="Limite de registros para ranking hospitalar"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
) -> Any:
    """
    Retorna o detalhamento aprofundado do Custo Total Hospitalar:
    - Decomposição SH (Serviços Hospitalares) vs. SP (Serviços Profissionais) vs. UTI;
    - Evolução Temporal dos componentes de custo;
    - Ranking de Hospitais CNES por volume financeiro;
    - Quebra por UF.
    """
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf) if uf else ""
    return cached_drilldown_custo_total(periodo=p, uf=u, limit=limit, offset=offset, request=request)


@router.get("/custo-desfecho")
async def get_drilldown_custo_desfecho(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: Optional[str] = Query(None, description="Sigla da UF opcional com 2 caracteres (ex: 'CE')"),
    limit: int = Query(50, ge=1, le=500, description="Limite de registros para ranking hospitalar"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
) -> Any:
    """
    Retorna o detalhamento aprofundado de Custo por Desfecho (Óbito vs. Alta):
    - Métricas comparativas de óbito vs alta (volume, custo total, custo médio, permanência média, razão de custo);
    - Curva de Gompertz-Makeham (mortalidade e custo por faixa etária);
    - Evolução Temporal da razão de custo óbito/alta;
    - Ranking hospitalar por desfechos e mortalidade.
    """
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf) if uf else ""
    return cached_drilldown_custo_desfecho(periodo=p, uf=u, limit=limit, offset=offset, request=request)
