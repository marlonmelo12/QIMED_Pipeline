"""
Endpoints analíticos da camada Gold do QIMED Lakehouse.
Rotas async de altíssimo desempenho com cache pré-serializado (orjson), pré-comprimido (gzip) e single-flight.
"""
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Query, Request, Response
from src.api.cache import (
    cached_query,
    cached_dashboard_financeiro,
    cached_central_anomalias,
    cached_drilldown_anomalia,
    cached_painel_glosa_ans,
    cached_anomalias_paginadas,
    cached_hospitais_eficiencia,
    get_cache_stats,
)

router = APIRouter(tags=["Analytics"])


def _sanitize_periodo(periodo: str) -> str:
    """Remove aspas simples para prevenção de SQL injection. Ex: '2026-05' → '2026-05'."""
    return periodo.replace("'", "").strip()


def _sanitize_uf(uf: str) -> str:
    """Normaliza UF para 2 chars maiúsculos sem aspas. Ex: 'ce' → 'CE'."""
    return uf.upper().replace("'", "").strip()[:2]


def _sanitize_param(val: Optional[str]) -> Optional[str]:
    """Sanitiza strings genéricas removendo aspas simples."""
    return val.replace("'", "").strip() if val else None


@router.get("/analytics/dashboard/financeiro")
async def get_dashboard_financeiro(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: Optional[str] = Query(None, description="Sigla da UF opcional com 2 caracteres (ex: 'CE')"),
) -> Any:
    """
    Retorna os dados consolidados para todos os widgets do Dashboard Financeiro em uma única requisição:
    1. Cards Superiores de KPI (Ticket Médio, Mediana, Custo Total, Razão Óbito vs Alta, Taxa de Glosa);
    2. Gráfico de Pareto de Glosas Hospitalares;
    3. Gráfico de Permanência (Custo e Dias Médios por Faixa);
    4. Série Temporal (AIHs Aprovadas vs Rejeitadas).
    """
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf) if uf else ""
    return cached_dashboard_financeiro(periodo=p, uf=u, request=request)


@router.get("/analytics/central-anomalias")
async def get_central_anomalias_grid(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    tipo: Optional[str] = Query(None, description="Filtro por tipo de anomalia (ex: 'OUTLIER_CUSTO_P99', 'AIH_VALOR_ZERO')"),
    prioridade: Optional[str] = Query(None, description="Filtro por prioridade/severidade (ex: 'CRITICA', 'ALTA', 'MEDIA')"),
    severidade: Optional[str] = Query(None, description="Alias para prioridade"),
    cnes: Optional[str] = Query(None, description="Filtro por código CNES do hospital"),
    status: Optional[str] = Query(None, description="Filtro por status (ex: 'NOVA', 'EM_ANALISE', 'RESOLVIDA')"),
    search: Optional[str] = Query(None, description="Busca textual por AIH, CNES, UF ou descrição"),
    limit: int = Query(20, ge=1, le=200, description="Quantidade de registros por página"),
    offset: int = Query(0, ge=0, description="Offset da paginação"),
) -> Any:
    """
    Retorna 100% dos dados para a tela operacional da Central de Anomalias (SIH):
    1. Cards Superiores de KPI (Anomalias Abertas, Valor em Risco, Taxa de Rejeição, Hospitais Afetados);
    2. Metadados de Paginação (Total de Registros, Páginas, Página Atual);
    3. Grid Humanizada de Anomalias com Filtros Dinâmicos e Busca Textual.
    """
    p = _sanitize_periodo(periodo)
    sev = _sanitize_param(prioridade or severidade)
    t = _sanitize_param(tipo)
    c = _sanitize_param(cnes)
    st = _sanitize_param(status)
    s = _sanitize_param(search)

    return cached_central_anomalias(
        periodo=p,
        tipo=t,
        severidade=sev,
        cnes=c,
        status=st,
        search=s,
        limit=limit,
        offset=offset,
        request=request,
    )


@router.get("/analytics/central-anomalias/{id_alerta}")
async def get_drilldown_central_anomalia(
    id_alerta: str,
    request: Request,
) -> Any:
    """
    Retorna o detalhamento aprofundado de um alerta individual para o modal de Drilldown:
    1. Dados do alerta e regra violada;
    2. Contexto do estabelecimento hospitalar e município do IBGE;
    3. Evolução temporal histórica no hospital;
    4. Amostra de AIHs correlacionadas para auditoria;
    5. Ações operacionais disponíveis.
    """
    clean_id = _sanitize_param(id_alerta) or ""
    return cached_drilldown_anomalia(id_alerta=clean_id, request=request)


@router.patch("/analytics/central-anomalias/{id_alerta}/status")
async def update_status_central_anomalia(
    id_alerta: str,
    status: str = Query(..., description="Novo status operacional ('NOVA', 'EM_ANALISE', 'RESOLVIDA', 'IGNORADA')"),
) -> Dict[str, Any]:
    """
    Atualiza transacionalmente o status de uma anomalia (workflow de auditoria) e invalida o cache do período.
    """
    clean_id = _sanitize_param(id_alerta) or ""
    clean_status = _sanitize_param(status) or ""
    
    from src.api.duckdb_query_engine import update_anomalia_status
    from src.api.cache import invalidate_cache_for_period
    
    resultado = update_anomalia_status(id_alerta=clean_id, novo_status=clean_status)
    if resultado.get("sucesso") and resultado.get("periodo"):
        invalidate_cache_for_period(resultado["periodo"])
        
    return resultado


@router.get("/analytics/painel-glosa-ans")
async def get_painel_glosa_ans(
    request: Request,
    periodo: Optional[str] = Query(None, description="Competência ou Ano (ex: '2025', '2026-05')"),
    visao: str = Query("setor", description="Visão analítica: 'setor' (com expurgo de outlier) ou 'operadora'"),
    segmentacao: Optional[str] = Query(None, description="Segmentação: 'Médico-Hospitalar' ou 'Odontológico'"),
    modalidade: Optional[str] = Query(None, description="Modalidade da operadora (ex: 'Cooperativa Médica', 'Autogestão')"),
    porte: Optional[str] = Query(None, description="Porte da operadora: 'Grande', 'Médio', 'Pequeno'"),
    registro_ans: Optional[str] = Query(None, description="Código de Registro ANS da operadora"),
    limit: int = Query(50, ge=1, le=200, description="Limite de registros para listagem paginada (visão operadora)"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
    threshold_mad: float = Query(3.5, ge=1.0, le=10.0, description="Threshold de Modified Z-score MAD para detecção de anomalia"),
    threshold_concentracao: float = Query(50.0, ge=10.0, le=100.0, description="Threshold de concentração setorial (%)"),
) -> Any:
    """
    Retorna 100% dos dados para a tela de Glosa Operadora (ANS) em uma única requisição HTTP:
    1. Os 5 Cards Superiores de KPI (Tempo Médio Pagamento, % Glosa Inicial, % Glosa Final, % Guias s/ Retorno 60d, % Valor s/ Retorno 60d);
    2. Detector Robusto de Operadora Atípica / Outlier via Modified Z-Score (MAD) com expurgo na média setorial;
    3. Detalhamento Multidimensional de % Glosa Inicial (por Porte, por Segmentação, por Modalidade);
    4. Paginação estrita quando visao='operadora'.
    """
    p = _sanitize_periodo(periodo) if periodo else ""
    v = _sanitize_param(visao) or "setor"
    seg = _sanitize_param(segmentacao)
    mod = _sanitize_param(modalidade)
    por = _sanitize_param(porte)
    ans = _sanitize_param(registro_ans)

    return cached_painel_glosa_ans(
        periodo=p,
        visao=v,
        segmentacao=seg,
        modalidade=mod,
        porte=por,
        registro_ans=ans,
        limit=limit,
        offset=offset,
        threshold_mad=threshold_mad,
        threshold_concentracao_pct=threshold_concentracao,
        request=request,
    )


@router.get("/analytics/glosas/operadoras")
async def get_glosas_operadoras(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    limit: int = Query(50, ge=1, le=200, description="Limite de registros"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
) -> Any:
    """Alias de compatibilidade que redireciona internamente para o endpoint oficial painel-glosa-ans (visao=operadora)."""
    p = _sanitize_periodo(periodo)
    return cached_painel_glosa_ans(
        periodo=p,
        visao="operadora",
        limit=limit,
        offset=offset,
        request=request,
    )


@router.get("/analytics/glosas/auditoria")
async def get_glosas_auditoria(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
) -> Any:
    """Retorna motivos e volumes de glosas hospitalares auditadas por UF."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    sql = f"SELECT * FROM dm_glosas_auditoria WHERE uf = '{u}' AND periodo = '{p}'"
    return cached_query(sql, periodo=p, request=request)


@router.get("/analytics/hospitais/eficiencia")
async def get_hospitais_eficiencia(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
    limit: Optional[int] = Query(None, ge=1, le=200, description="Limite opcional de registros por página"),
    offset: int = Query(0, ge=0, description="Offset da paginação"),
) -> Any:
    """Retorna métricas de eficiência hospitalar, ocupação e mortalidade por estabelecimento com suporte a paginação."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    return cached_hospitais_eficiencia(periodo=p, uf=u, limit=limit, offset=offset, request=request)


@router.get("/analytics/icsap")
async def get_icsap(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
) -> Any:
    """Retorna indicadores de Internações por Condições Sensíveis à Atenção Primária (ICSAP)."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    sql = f"SELECT * FROM dm_icsap_prevention WHERE uf = '{u}' AND periodo = '{p}'"
    return cached_query(sql, periodo=p, request=request)


@router.get("/analytics/anomalias")
async def get_anomalias(
    request: Request,
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    limit: int = Query(50, ge=1, le=200, description="Quantidade máxima de anomalias por página (teto: 200)"),
    offset: int = Query(0, ge=0, description="Offset de paginação"),
) -> Any:
    """Retorna alertas forenses e anomalias de custo faturado (P99) com paginação estrita."""
    p = _sanitize_periodo(periodo)
    return cached_anomalias_paginadas(periodo=p, limit=limit, offset=offset, request=request)


@router.get("/analytics/cache/stats")
async def get_cache_observability_stats() -> Dict[str, Any]:
    """Retorna métricas de telemetria, taxa de acerto de cache, tempo de reconstrução e memória L1."""
    return get_cache_stats()
