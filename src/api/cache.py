"""
Módulo de Cache de Alta Performance, Single-Flight e Multi-Worker para Consultas Analíticas QIMED.

Características arquiteturais:
1. Armazena payloads pré-serializados (orjson) e pré-comprimidos (gzip), eliminando custos de CPU no cache hit.
2. Proteção contra Cache Stampede via Single-Flight por chave (apenas 1 worker/thread reconstrói o cache).
3. Invalidação atômica multi-worker via Epoch/Watermark Versioning.
4. Compatibilidade bidirecional: serve FastAPI Response de alta velocidade quando chamado com Request,
   ou retorna dicionários Python puros para compatibilidade com testes e chamadas internas.
5. Controle estrito de memória L1 com limites de tamanho de payload e LRU/TTL.
"""
import os
import json
import gzip
import time
import uuid
import decimal
import datetime
import hashlib
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cachetools import TTLCache
from fastapi import Request, Response
from src.utils.logging_config import setup_logger
import src.api.duckdb_query_engine as query_engine

logger = setup_logger(__name__)

# Tenta carregar orjson para serialização ultrarrápida compilada em C/Rust
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    orjson = None  # type: ignore
    HAS_ORJSON = False

# Configurações de Cache e Limites de Memória
_TTL_SECONDS = int(os.getenv("ANALYTICS_CACHE_TTL_SECONDS", "3600"))
_MAX_ENTRIES = int(os.getenv("ANALYTICS_CACHE_MAX_ENTRIES", "1024"))
_MAX_PAYLOAD_SIZE_BYTES = int(os.getenv("ANALYTICS_CACHE_MAX_PAYLOAD_BYTES", "10485760"))  # 10 MB
_GZIP_MIN_SIZE = int(os.getenv("GZIP_MIN_SIZE", "512"))  # 512 bytes
_GZIP_COMPRESS_LEVEL = int(os.getenv("GZIP_COMPRESS_LEVEL", "5"))

_VERSION_FILE = os.getenv("CACHE_VERSION_FILE", "warehouse/.cache_version.json")
_REDIS_URL = os.getenv("REDIS_URL")

# Estrutura de Payload Pré-Compilado
@dataclass(slots=True)
class CachedPayload:
    epoch: int
    raw_json_bytes: bytes
    gzip_bytes: bytes
    raw_size: int
    gzip_size: int
    data_obj: Any
    created_at: float


# Cache L1 em memória
_cache: TTLCache = TTLCache(maxsize=_MAX_ENTRIES, ttl=_TTL_SECONDS)
_cache_lock = threading.Lock()
_lock = _cache_lock  # Alias para compatibilidade com testes unitários

# Single-Flight: locks granulares por chave para evitar Cache Stampede
_key_locks: Dict[str, threading.Lock] = {}
_key_locks_registry_lock = threading.Lock()

# Métricas e Observabilidade
_metrics = {
    "requests_total": 0,
    "cache_hit_gzip": 0,
    "cache_hit_raw": 0,
    "cache_miss": 0,
    "single_flight_waits": 0,
    "rebuilds_count": 0,
    "rebuild_total_duration_ms": 0.0,
}
_metrics_lock = threading.Lock()

# Cache local de epochs
_local_epoch_cache: Dict[str, int] = {}
_last_epoch_read_time: float = 0.0
_EPOCH_REFRESH_INTERVAL = 0.2  # 200ms


def _get_version_file_path() -> str:
    return os.getenv("CACHE_VERSION_FILE", "warehouse/.cache_version.json")


def _get_shared_epochs() -> Dict[str, int]:
    """Lê o estado compartilhado de versões/epochs do arquivo de controle ou Redis."""
    global _local_epoch_cache, _last_epoch_read_time
    now = time.time()
    if now - _last_epoch_read_time < _EPOCH_REFRESH_INTERVAL and _local_epoch_cache:
        return _local_epoch_cache

    version_file = _get_version_file_path()

    # 1. Redis compartilhado se configurado
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        try:
            import redis
            r = redis.Redis.from_url(redis_url, socket_timeout=1.0)
            raw = r.hgetall("qimed:cache:epochs")
            if raw:
                _local_epoch_cache = {k.decode("utf-8"): int(v.decode("utf-8")) for k, v in raw.items()}
                _last_epoch_read_time = now
                return _local_epoch_cache
        except Exception:
            pass

    # 2. Arquivo compartilhado no volume (atômico entre workers)
    if os.path.exists(version_file):
        try:
            with open(version_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _local_epoch_cache = data
                    _last_epoch_read_time = now
                    return _local_epoch_cache
        except Exception:
            pass

    return _local_epoch_cache or {"__global__": 1}


def get_cache_epoch(periodo: str = "") -> int:
    """Retorna o número de versão/epoch atual para um dado período."""
    epochs = _get_shared_epochs()
    global_epoch = epochs.get("__global__", 1)
    periodo_epoch = epochs.get(periodo, 1) if periodo else 1
    return max(global_epoch, periodo_epoch)


def invalidate_cache_for_period(periodo: str) -> None:
    """Invalida atomicamente o cache de uma competência específica para TODOS os workers."""
    global _last_epoch_read_time
    with _cache_lock:
        epochs = _get_shared_epochs().copy()
        current = epochs.get(periodo, 1)
        epochs[periodo] = current + 1
        epochs["__global__"] = epochs.get("__global__", 1) + 1

        version_file = _get_version_file_path()
        os.makedirs(os.path.dirname(version_file) if os.path.dirname(version_file) else ".", exist_ok=True)
        tmp_file = f"{version_file}.tmp.{int(time.time()*1000)}"
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(epochs, f)
            os.replace(tmp_file, version_file)
        except Exception as e:
            logger.warning(f"Erro ao salvar versão de cache: {e}")
            if os.path.exists(tmp_file):
                try: os.remove(tmp_file)
                except Exception: pass

        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis
                r = redis.Redis.from_url(redis_url, socket_timeout=1.0)
                r.hincrby("qimed:cache:epochs", periodo, 1)
                r.hincrby("qimed:cache:epochs", "__global__", 1)
            except Exception:
                pass

        _cache.clear()
        _last_epoch_read_time = 0.0
        logger.info(f"[CACHE INVALIDATION] Cache invalidado para o período '{periodo}' (novo epoch: {epochs[periodo]}).")


def invalidate_all_cache() -> None:
    """Invalida globalmente o cache para todos os períodos e workers."""
    invalidate_cache_for_period("__all__")


def _custom_json_default(obj: Any) -> Any:
    """Serializador customizado para tipos complexos (Decimal, datetime, UUID)."""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if hasattr(obj, "dict"):
        return obj.dict()
    if hasattr(obj, "tolist"):
        return obj.tolist()
    raise TypeError(f"Objeto do tipo {type(obj)} não é serializável em JSON")


def serialize_json(data: Any) -> bytes:
    """Serializa estruturas Python em bytes UTF-8 com orjson (se disponível) ou json stdlib."""
    if HAS_ORJSON and orjson is not None:
        return orjson.dumps(data, default=_custom_json_default, option=orjson.OPT_NON_STR_KEYS)
    return json.dumps(data, default=_custom_json_default, ensure_ascii=False).encode("utf-8")


def _generate_cache_key(prefix: str, *args, **kwargs) -> str:
    """Gera chave SHA-256 de 16 caracteres a partir dos parâmetros de consulta."""
    raw_str = f"{prefix}:" + ":".join(str(a) for a in args) + ":" + ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
    return hashlib.sha256(raw_str.strip().lower().encode("utf-8")).hexdigest()[:16]


def _get_key_lock(key: str) -> threading.Lock:
    """Obtém ou cria um lock individual por chave para o Single-Flight."""
    with _key_locks_registry_lock:
        if key not in _key_locks:
            _key_locks[key] = threading.Lock()
        return _key_locks[key]


def _build_and_cache_payload(key: str, epoch: int, data: Any) -> CachedPayload:
    """Serializa, pré-comprime com GZip e armazena o payload no cache L1."""
    t0 = time.perf_counter()
    raw_bytes = serialize_json(data)
    raw_size = len(raw_bytes)

    # Comprime com Gzip se ultrapassar o tamanho mínimo
    if raw_size >= _GZIP_MIN_SIZE:
        gzip_bytes = gzip.compress(raw_bytes, compresslevel=_GZIP_COMPRESS_LEVEL)
    else:
        gzip_bytes = raw_bytes
    gzip_size = len(gzip_bytes)

    payload = CachedPayload(
        epoch=epoch,
        raw_json_bytes=raw_bytes,
        gzip_bytes=gzip_bytes,
        raw_size=raw_size,
        gzip_size=gzip_size,
        data_obj=data,
        created_at=time.time(),
    )

    # Controle de Memória: apenas armazena se respeitar o limite máximo por payload
    if raw_size <= _MAX_PAYLOAD_SIZE_BYTES:
        with _cache_lock:
            _cache[key] = payload

    dur_ms = (time.perf_counter() - t0) * 1000
    with _metrics_lock:
        _metrics["rebuilds_count"] += 1
        _metrics["rebuild_total_duration_ms"] += dur_ms

    return payload


def get_cached_payload_single_flight(
    key: str,
    cache_period: str,
    builder_func: Callable[..., Any],
    *args,
    **kwargs,
) -> CachedPayload:
    """
    Executa o padrão Single-Flight com Double-Checked Locking:
    - Se o cache hit for válido, retorna imediatamente.
    - Em caso de miss, adquire lock EXCLUSIVO para a chave específica.
    - Outras requisições concorrentes para a mesma chave aguardam e reutilizam o resultado construído.
    """
    epoch = get_cache_epoch(cache_period)

    # 1. Leitura rápida sem lock de chave
    with _cache_lock:
        if key in _cache:
            item = _cache[key]
            entry_epoch = item.epoch if isinstance(item, CachedPayload) else (item[0] if isinstance(item, tuple) else 0)
            if entry_epoch >= epoch:
                if isinstance(item, CachedPayload):
                    return item
                return _build_and_cache_payload(key, epoch, item[1] if isinstance(item, tuple) else item)

    # 2. Single-Flight: adquire lock específico da chave
    key_lock = _get_key_lock(key)
    with key_lock:
        # Double-check: outra thread pode ter acabado de reconstruir enquanto aguardávamos
        with _cache_lock:
            if key in _cache:
                item = _cache[key]
                entry_epoch = item.epoch if isinstance(item, CachedPayload) else (item[0] if isinstance(item, tuple) else 0)
                if entry_epoch >= epoch:
                    with _metrics_lock:
                        _metrics["single_flight_waits"] += 1
                    if isinstance(item, CachedPayload):
                        return item
                    return _build_and_cache_payload(key, epoch, item[1] if isinstance(item, tuple) else item)

        # Miss real: executa a query e constrói o payload pré-comprimido
        with _metrics_lock:
            _metrics["cache_miss"] += 1

        data = builder_func(*args, **kwargs)
        return _build_and_cache_payload(key, epoch, data)


def serve_cached_http_response(
    request: Optional[Request],
    key: str,
    cache_period: str,
    builder_func: Callable[..., Any],
    *args,
    **kwargs,
) -> Union[Response, Any]:
    """
    Ponto de entrada de altíssimo desempenho para rotas FastAPI:
    - Se chamado com um `Request` HTTP, retorna uma `Response` pré-comprimida (Zero CPU overhead).
    - Se chamado sem `Request` (ex: testes unitários), retorna o `data_obj` original.
    """
    with _metrics_lock:
        _metrics["requests_total"] += 1

    payload = get_cached_payload_single_flight(key, cache_period, builder_func, *args, **kwargs)

    if request is None:
        return payload.data_obj

    accept_encoding = request.headers.get("accept-encoding", "").lower() if request else ""
    client_supports_gzip = "gzip" in accept_encoding and payload.gzip_size < payload.raw_size

    if client_supports_gzip:
        with _metrics_lock:
            _metrics["cache_hit_gzip"] += 1
        return Response(
            content=payload.gzip_bytes,
            status_code=200,
            media_type="application/json",
            headers={
                "Content-Encoding": "gzip",
                "Vary": "Accept-Encoding",
                "Content-Length": str(payload.gzip_size),
                "X-Cache": "HIT-GZIP",
                "X-Cache-Epoch": str(payload.epoch),
            },
        )
    else:
        with _metrics_lock:
            _metrics["cache_hit_raw"] += 1
        return Response(
            content=payload.raw_json_bytes,
            status_code=200,
            media_type="application/json",
            headers={
                "Vary": "Accept-Encoding",
                "Content-Length": str(payload.raw_size),
                "X-Cache": "HIT-RAW",
                "X-Cache-Epoch": str(payload.epoch),
            },
        )


# ==============================================================================
# Funções de Conveniência para os Endpoints Analíticos
# ==============================================================================

def cached_query(sql: str, periodo: str = "", request: Optional[Request] = None) -> Union[Response, list]:
    key = _generate_cache_key("sql", sql)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_gold,
        sql=sql,
    )


def cached_dashboard_financeiro(periodo: str, uf: str = "", request: Optional[Request] = None) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("dash_fin", periodo, uf)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_dashboard_financeiro,
        periodo=periodo,
        uf=uf,
    )


def cached_drilldown_ticket_medio(periodo: str, uf: str = "", limit: int = 50, offset: int = 0, request: Optional[Request] = None) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("dd_tm", periodo, uf, limit, offset)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_drilldown_ticket_medio,
        periodo=periodo,
        uf=uf,
        limit=limit,
        offset=offset,
    )


def cached_drilldown_custo_total(periodo: str, uf: str = "", limit: int = 50, offset: int = 0, request: Optional[Request] = None) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("dd_ct", periodo, uf, limit, offset)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_drilldown_custo_total,
        periodo=periodo,
        uf=uf,
        limit=limit,
        offset=offset,
    )


def cached_drilldown_custo_desfecho(periodo: str, uf: str = "", limit: int = 50, offset: int = 0, request: Optional[Request] = None) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("dd_cd", periodo, uf, limit, offset)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_drilldown_custo_desfecho,
        periodo=periodo,
        uf=uf,
        limit=limit,
        offset=offset,
    )


def cached_central_anomalias(
    periodo: str,
    tipo: Optional[str] = None,
    severidade: Optional[str] = None,
    cnes: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    request: Optional[Request] = None,
) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("cent_anom", periodo, tipo, severidade, cnes, status, search, limit, offset)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_central_anomalias,
        periodo=periodo,
        tipo=tipo,
        severidade=severidade,
        cnes=cnes,
        status=status,
        search=search,
        limit=limit,
        offset=offset,
    )


def cached_painel_glosa_ans(
    periodo: str = "",
    visao: str = "setor",
    segmentacao: Optional[str] = None,
    modalidade: Optional[str] = None,
    porte: Optional[str] = None,
    registro_ans: Optional[str] = None,
    request: Optional[Request] = None,
) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("painel_glosa_ans", periodo, visao, segmentacao, modalidade, porte, registro_ans)
    return serve_cached_http_response(
        request=request,
        key=key,
        cache_period=periodo,
        builder_func=query_engine.query_painel_glosa_ans,
        periodo=periodo,
        visao=visao,
        segmentacao=segmentacao,
        modalidade=modalidade,
        porte=porte,
        registro_ans=registro_ans,
    )


def cached_anomalias_paginadas(
    periodo: str,
    limit: int = 50,
    offset: int = 0,
    request: Optional[Request] = None,
) -> Union[Response, Dict[str, Any]]:
    key = _generate_cache_key("anom_pag", periodo, limit, offset)
    def _builder():
        sql_count = f"SELECT COUNT(*) AS total FROM aud_alertas_anomalias WHERE periodo = '{periodo}'"
        sql_data = f"SELECT * FROM aud_alertas_anomalias WHERE periodo = '{periodo}' ORDER BY criado_em DESC LIMIT {limit} OFFSET {offset}"
        total_res = query_engine.query_gold(sql_count)
        total_registros = total_res[0].get("total", len(total_res)) if total_res and isinstance(total_res, list) and isinstance(total_res[0], dict) else 0
        itens = query_engine.query_gold(sql_data)
        total_paginas = (total_registros + limit - 1) // limit if limit > 0 else 1
        pagina_atual = (offset // limit) + 1 if limit > 0 else 1
        return {
            "paginacao": {
                "total_registros": total_registros,
                "pagina_atual": pagina_atual,
                "total_paginas": total_paginas,
                "limit": limit,
                "offset": offset,
            },
            "itens": itens,
        }
    return serve_cached_http_response(request, key, cache_period=periodo, builder_func=_builder)


def cached_hospitais_eficiencia(
    periodo: str,
    uf: str,
    limit: Optional[int] = None,
    offset: int = 0,
    request: Optional[Request] = None,
) -> Union[Response, Any]:
    key = _generate_cache_key("hosp_ef", periodo, uf, limit, offset)
    def _builder():
        if limit is not None:
            sql = f"SELECT * FROM dm_hospital_efficiency WHERE uf = '{uf}' AND periodo = '{periodo}' LIMIT {limit} OFFSET {offset}"
        else:
            sql = f"SELECT * FROM dm_hospital_efficiency WHERE uf = '{uf}' AND periodo = '{periodo}'"
        return query_engine.query_gold(sql)
    return serve_cached_http_response(request, key, cache_period=periodo, builder_func=_builder)


def get_cache_stats() -> Dict[str, Any]:
    """Retorna métricas de performance e memória do cache L1."""
    with _metrics_lock:
        stats = _metrics.copy()
    with _cache_lock:
        stats["entries_count"] = len(_cache)
        stats["max_entries"] = _MAX_ENTRIES
        total_raw_bytes = sum(getattr(item, "raw_size", 0) for item in _cache.values() if isinstance(item, CachedPayload))
        total_gzip_bytes = sum(getattr(item, "gzip_size", 0) for item in _cache.values() if isinstance(item, CachedPayload))
        stats["total_raw_bytes"] = total_raw_bytes
        stats["total_gzip_bytes"] = total_gzip_bytes
        stats["total_memory_mb"] = round(total_raw_bytes / (1024 * 1024), 3)
    return stats
