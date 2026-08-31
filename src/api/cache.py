"""
Módulo de Cache TTL em Memória para Consultas Analíticas DuckDB.
Garante thread-safety com threading.Lock e tempo de expiração configurável.
"""
import os
import hashlib
import threading
from typing import Any, Dict, Optional
from cachetools import TTLCache
import src.api.duckdb_query_engine as query_engine

_TTL_SECONDS = int(os.getenv("ANALYTICS_CACHE_TTL_SECONDS", "3600"))
_MAX_SIZE = int(os.getenv("ANALYTICS_CACHE_MAX_SIZE", "1024"))

_cache: TTLCache = TTLCache(maxsize=_MAX_SIZE, ttl=_TTL_SECONDS)
_lock = threading.Lock()


def _generate_cache_key(sql_or_id: str) -> str:
    """Gera chave SHA-256 de 16 caracteres a partir da query/identificador normalizado."""
    return hashlib.sha256(sql_or_id.strip().lower().encode("utf-8")).hexdigest()[:16]


def cached_query(sql: str) -> list[dict]:
    """
    Consulta o cache antes de delegar a execução ao DuckDB.
    Thread-safe com proteção de lock.
    """
    key = _generate_cache_key(sql)
    with _lock:
        if key in _cache:
            return _cache[key]

    result = query_engine.query_gold(sql)

    with _lock:
        _cache[key] = result

    return result


def cached_dashboard_financeiro(periodo: str, uf: str = "") -> Dict[str, Any]:
    """
    Consulta o cache do payload consolidado do Dashboard Financeiro.
    Thread-safe com chave composta normalizada.
    """
    cache_key = _generate_cache_key(f"dash_fin_{periodo}_{uf}")
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_dashboard_financeiro(periodo=periodo, uf=uf)

    with _lock:
        _cache[cache_key] = result

    return result


def cached_drilldown_ticket_medio(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """Cache thread-safe para o Drill-down de Ticket Médio."""
    cache_key = _generate_cache_key(f"dd_tm_{periodo}_{uf}_{limit}_{offset}")
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_drilldown_ticket_medio(periodo=periodo, uf=uf, limit=limit, offset=offset)

    with _lock:
        _cache[cache_key] = result

    return result


def cached_drilldown_custo_total(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """Cache thread-safe para o Drill-down de Custo Total."""
    cache_key = _generate_cache_key(f"dd_ct_{periodo}_{uf}_{limit}_{offset}")
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_drilldown_custo_total(periodo=periodo, uf=uf, limit=limit, offset=offset)

    with _lock:
        _cache[cache_key] = result

    return result


def cached_drilldown_custo_desfecho(periodo: str, uf: str = "", limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    """Cache thread-safe para o Drill-down de Custo por Desfecho."""
    cache_key = _generate_cache_key(f"dd_cd_{periodo}_{uf}_{limit}_{offset}")
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_drilldown_custo_desfecho(periodo=periodo, uf=uf, limit=limit, offset=offset)

    with _lock:
        _cache[cache_key] = result

    return result


def cached_central_anomalias(
    periodo: str,
    tipo: Optional[str] = None,
    severidade: Optional[str] = None,
    cnes: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """Cache thread-safe para a tela operacional da Central de Anomalias."""
    cache_key = _generate_cache_key(f"cent_anom_{periodo}_{tipo}_{severidade}_{cnes}_{status}_{search}_{limit}_{offset}")
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_central_anomalias(
        periodo=periodo,
        tipo=tipo,
        severidade=severidade,
        cnes=cnes,
        status=status,
        search=search,
        limit=limit,
        offset=offset,
    )

    with _lock:
        _cache[cache_key] = result

    return result


def cached_painel_glosa_ans(
    periodo: str = "",
    visao: str = "setor",
    segmentacao: Optional[str] = None,
    modalidade: Optional[str] = None,
    porte: Optional[str] = None,
    registro_ans: Optional[str] = None,
) -> Dict[str, Any]:
    """Cache thread-safe para o Painel de Glosas Operadora (ANS)."""
    cache_key = _generate_cache_key(
        f"painel_glosa_ans_{periodo}_{visao}_{segmentacao}_{modalidade}_{porte}_{registro_ans}"
    )
    with _lock:
        if cache_key in _cache:
            return _cache[cache_key]

    result = query_engine.query_painel_glosa_ans(
        periodo=periodo,
        visao=visao,
        segmentacao=segmentacao,
        modalidade=modalidade,
        porte=porte,
        registro_ans=registro_ans,
    )

    with _lock:
        _cache[cache_key] = result

    return result


