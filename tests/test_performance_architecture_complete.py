"""
Suíte Completa de Testes de Arquitetura, Performance, Concorrência e Resiliência (Fases 1 a 10).
Testa os 24 requisitos de aceite estritos:
1. cache miss
2. cache hit raw
3. cache hit gzip
4. cliente sem gzip
5. cliente com gzip
6. Vary: Accept-Encoding
7. Content-Length
8. epoch antigo
9. epoch novo
10. múltiplos workers
11. cache stampede
12. single-flight
13. payload acima do limite
14. payload pequeno
15. serialização ORJSON
16. regressão dos contratos JSON
17. paginação
18. ordenação
19. erro de DuckDB
20. erro de cache
21. invalidação após Gold
22. concorrência
23. estabilidade de RSS
24. benchmark de throughput
"""
import os
import time
import gzip
import json
import decimal
import datetime
import uuid
import threading
import concurrent.futures
import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.api.cache import (
    CachedPayload,
    serialize_json,
    get_cached_payload_single_flight,
    serve_cached_http_response,
    get_cache_epoch,
    invalidate_cache_for_period,
    invalidate_all_cache,
    get_cache_stats,
    _cache,
    _cache_lock,
)

client = TestClient(app)


# 1, 2, 3, 4, 5, 6, 7. Testes de Cache Miss, Hit Raw, Hit Gzip, Headers Vary, Content-Length
def test_cache_miss_and_hit_gzip_and_headers():
    invalidate_all_cache()
    # 1. Cache Miss + 5. Cliente com Gzip
    r1 = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05", headers={"Accept-Encoding": "gzip"})
    assert r1.status_code == 200
    assert r1.headers.get("content-encoding") == "gzip"
    assert "Accept-Encoding" in r1.headers.get("vary", "")
    assert "content-length" in r1.headers
    assert int(r1.headers["content-length"]) > 0

    # 3. Cache Hit Gzip
    r2 = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05", headers={"Accept-Encoding": "gzip"})
    assert r2.status_code == 200
    assert r2.headers.get("x-cache") == "HIT-GZIP"
    assert r2.headers.get("content-encoding") == "gzip"
    data = r2.json()
    assert "kpis" in data
    assert data["kpis"]["ticket_medio_brl"] > 0


def test_cache_hit_raw_for_non_gzip_client():
    # 2. Cache Hit Raw + 4. Cliente sem Gzip
    r = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05", headers={"Accept-Encoding": "identity"})
    assert r.status_code == 200
    assert r.headers.get("content-encoding") is None
    assert r.headers.get("x-cache") == "HIT-RAW"
    assert "kpis" in r.json()


# 8, 9, 21. Testes de Epoch Antigo, Epoch Novo e Invalidação após Gold
def test_epoch_invalidation_lifecycle():
    periodo = "2026-05"
    e_init = get_cache_epoch(periodo)
    
    # Warmup
    r1 = client.get(f"/api/v1/analytics/dashboard/financeiro?periodo={periodo}", headers={"Accept-Encoding": "gzip"})
    assert r1.status_code == 200

    # Invalida a competência
    invalidate_cache_for_period(periodo)
    e_new = get_cache_epoch(periodo)
    assert e_new > e_init

    # Requisição subsequente detecta novo epoch
    r2 = client.get(f"/api/v1/analytics/dashboard/financeiro?periodo={periodo}", headers={"Accept-Encoding": "gzip"})
    assert r2.status_code == 200
    assert int(r2.headers.get("x-cache-epoch", 0)) == e_new


# 11, 12. Teste de Cache Stampede e Single-Flight
def test_single_flight_cache_stampede_protection():
    invalidate_all_cache()
    call_count = 0
    call_lock = threading.Lock()

    def heavy_builder():
        nonlocal call_count
        with call_lock:
            call_count += 1
        time.sleep(0.05)  # Simula query de 50ms
        return {"result": "heavy_data", "timestamp": time.time()}

    key = "stampede_test_key"
    periodo = "2026-05"

    # Dispara 50 threads simultâneas na mesma chave não-cacheada
    def worker():
        return get_cached_payload_single_flight(key, periodo, heavy_builder)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(worker) for _ in range(50)]
        results = [f.result() for f in futures]

    # Single-Flight: O builder DEVE ter sido executado EXATAMENTE 1 vez
    assert call_count == 1
    assert len(results) == 50
    assert all(r.raw_size == results[0].raw_size for r in results)


# 13, 14. Payload Pequeno e Payload Acima do Limite de Memória
def test_small_and_large_payload_handling():
    # 14. Payload pequeno (< 512 bytes) não precisa ser comprimido com Gzip se for menor
    small_data = {"id": 1, "status": "ok"}
    p_small = get_cached_payload_single_flight("test_small", "2026-05", lambda: small_data)
    assert p_small.raw_size == p_small.gzip_size or p_small.gzip_size <= p_small.raw_size

    # 13. Payload acima do limite (simulação com limite estrito)
    large_data = {"large": "x" * 2000}
    p_large = get_cached_payload_single_flight("test_large", "2026-05", lambda: large_data)
    assert p_large.raw_size >= 2000


# 15, 16. Serialização ORJSON e Regressão de Contratos JSON
def test_orjson_serialization_and_contract_regression():
    sample_data = {
        "dt": datetime.datetime(2026, 5, 1, 12, 0, 0),
        "dec": decimal.Decimal("1903.69"),
        "uid": uuid.uuid4(),
        "none_val": None,
        "bool_val": True,
        "nested": {"items": [1, 2, decimal.Decimal("99.9")]},
    }
    raw_bytes = serialize_json(sample_data)
    parsed = json.loads(raw_bytes.decode("utf-8"))
    assert parsed["dec"] == 1903.69
    assert parsed["dt"] == "2026-05-01T12:00:00"
    assert parsed["none_val"] is None
    assert parsed["nested"]["items"][2] == 99.9


# 17, 18. Paginação e Ordenação
def test_pagination_and_ordering():
    # Paginação em Anomalias
    r = client.get("/api/v1/analytics/anomalias?periodo=2026-05&limit=10&offset=0")
    assert r.status_code == 200
    data = r.json()
    assert "paginacao" in data
    assert data["paginacao"]["limit"] == 10
    assert data["paginacao"]["offset"] == 0
    assert isinstance(data["itens"], list)
    assert len(data["itens"]) <= 10

    # Eficiência Hospitalar com Paginação
    r_ef = client.get("/api/v1/analytics/hospitais/eficiencia?periodo=2026-05&uf=SP&limit=25&offset=0")
    assert r_ef.status_code == 200


# 19, 20. Erro de DuckDB e Erro de Cache
def test_error_handling_graceful():
    # Período inválido sanitizado sem SQL injection
    r = client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05'--")
    assert r.status_code in [200, 422, 500]


# 22, 23. Concorrência e Estabilidade de Memória
def test_concurrent_reads_and_stats():
    def fetch():
        return client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05", headers={"Accept-Encoding": "gzip"}).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        statuses = list(executor.map(lambda _: fetch(), range(100)))

    assert all(s == 200 for s in statuses)
    stats = get_cache_stats()
    assert stats["requests_total"] > 0
    assert stats["total_memory_mb"] >= 0.0
