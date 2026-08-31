"""
Endpoints analíticos da camada Gold do QIMED Lakehouse.
Consultas somente leitura com cache em memória e sanitização de parâmetros.

Todas as tabelas dm_* possuem coluna `periodo` no formato 'YYYY-MM'.
NÃO use colunas `ano`/`mes` separadas aqui — essas existem apenas na Silver.
"""
from typing import Any, Dict, Optional
from fastapi import APIRouter, Query
from src.api.cache import (
    cached_query,
    cached_dashboard_financeiro,
    cached_central_anomalias,
    cached_painel_glosa_ans,
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
def get_dashboard_financeiro(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: Optional[str] = Query(None, description="Sigla da UF opcional com 2 caracteres (ex: 'CE')"),
) -> Dict[str, Any]:
    """
    Retorna os dados consolidados para todos os widgets do Dashboard Financeiro em uma única requisição:
    1. Cards Superiores de KPI (Ticket Médio, Mediana, Custo Total, Razão Óbito vs Alta, Taxa de Glosa);
    2. Gráfico de Pareto de Glosas Hospitalares;
    3. Gráfico de Permanência (Custo e Dias Médios por Faixa);
    4. Série Temporal (AIHs Aprovadas vs Rejeitadas).
    """
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf) if uf else ""
    return cached_dashboard_financeiro(periodo=p, uf=u)


@router.get("/analytics/central-anomalias")
def get_central_anomalias_grid(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    tipo: Optional[str] = Query(None, description="Filtro por tipo de anomalia (ex: 'OUTLIER_CUSTO_P99', 'AIH_VALOR_ZERO')"),
    prioridade: Optional[str] = Query(None, description="Filtro por prioridade/severidade (ex: 'CRITICA', 'ALTA', 'MEDIA')"),
    severidade: Optional[str] = Query(None, description="Alias para prioridade"),
    cnes: Optional[str] = Query(None, description="Filtro por código CNES do hospital"),
    status: Optional[str] = Query(None, description="Filtro por status (ex: 'NOVA', 'EM_ANALISE', 'RESOLVIDA')"),
    search: Optional[str] = Query(None, description="Busca textual por AIH, CNES, UF ou descrição"),
    limit: int = Query(20, ge=1, le=200, description="Quantidade de registros por página"),
    offset: int = Query(0, ge=0, description="Offset da paginação"),
) -> Dict[str, Any]:
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
    )


@router.get("/analytics/painel-glosa-ans")
def get_painel_glosa_ans(
    periodo: Optional[str] = Query(None, description="Competência ou Ano (ex: '2025', '2026-05')"),
    visao: str = Query("setor", description="Visão analítica: 'setor' (com expurgo de outlier) ou 'operadora'"),
    segmentacao: Optional[str] = Query(None, description="Segmentação: 'Médico-Hospitalar' ou 'Odontológico'"),
    modalidade: Optional[str] = Query(None, description="Modalidade da operadora (ex: 'Cooperativa Médica', 'Autogestão')"),
    porte: Optional[str] = Query(None, description="Porte da operadora: 'Grande', 'Médio', 'Pequeno'"),
    registro_ans: Optional[str] = Query(None, description="Código de Registro ANS da operadora"),
) -> Dict[str, Any]:
    """
    Retorna 100% dos dados para a tela de Glosa Operadora (ANS) em uma única requisição HTTP:
    1. Os 5 Cards Superiores de KPI (Tempo Médio Pagamento, % Glosa Inicial, % Glosa Final, % Guias s/ Retorno 60d, % Valor s/ Retorno 60d);
    2. Detector de Operadora Atípica / Outlier (>90% de concentração com expurgo na média setorial);
    3. Detalhamento Multidimensional de % Glosa Inicial (por Porte, por Segmentação, por Modalidade).
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
    )


@router.get("/analytics/glosas/operadoras")
def get_glosas_operadoras(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
) -> list[dict]:
    """Retorna indicadores consolidados de glosas por operadora de saúde (ANS/TISS)."""
    p = _sanitize_periodo(periodo)
    sql = f"SELECT * FROM dm_ans_glosas_operadoras WHERE periodo = '{p}' ORDER BY taxa_glosa_pct DESC"
    return cached_query(sql)


@router.get("/analytics/glosas/auditoria")
def get_glosas_auditoria(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
) -> list[dict]:
    """Retorna motivos e volumes de glosas hospitalares auditadas por UF."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    sql = f"SELECT * FROM dm_glosas_auditoria WHERE uf = '{u}' AND periodo = '{p}'"
    return cached_query(sql)


@router.get("/analytics/hospitais/eficiencia")
def get_hospitais_eficiencia(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
) -> list[dict]:
    """Retorna métricas de eficiência hospitalar, ocupação e mortalidade por estabelecimento."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    sql = f"SELECT * FROM dm_hospital_efficiency WHERE uf = '{u}' AND periodo = '{p}'"
    return cached_query(sql)


@router.get("/analytics/icsap")
def get_icsap(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
    uf: str = Query(..., description="Sigla da UF com 2 caracteres (ex: 'CE')"),
) -> list[dict]:
    """Retorna indicadores de Internações por Condições Sensíveis à Atenção Primária (ICSAP)."""
    p = _sanitize_periodo(periodo)
    u = _sanitize_uf(uf)
    sql = f"SELECT * FROM dm_icsap_prevention WHERE uf = '{u}' AND periodo = '{p}'"
    return cached_query(sql)


@router.get("/analytics/anomalias")
def get_anomalias(
    periodo: str = Query(..., description="Competência no formato YYYY-MM (ex: '2026-05')"),
) -> list[dict]:
    """Retorna alertas forenses e anomalias de custo faturado (P99)."""
    p = _sanitize_periodo(periodo)
    sql = f"SELECT * FROM aud_alertas_anomalias WHERE periodo = '{p}' ORDER BY criado_em DESC"
    return cached_query(sql)

