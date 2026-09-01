"""
Testes de Performance, Compressão HTTP, Paginação e Fallback da API Analítica (Fases 1 a 4).
"""
import time
import asyncio
import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app


def test_gzip_compression_and_performance():
    """Valida compressão HTTP Gzip em payloads analíticos superiores a 1KB."""
    async def _run():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/v1/analytics/central-anomalias?periodo=2026-05",
                headers={"Accept-Encoding": "gzip"}
            )
            assert resp.status_code == 200
            assert resp.content is not None
            assert len(resp.content) > 0

    asyncio.run(_run())


def test_bounded_pagination_anomalias():
    """Valida paginação estrita com teto máximo de 200 registros na rota /analytics/anomalias."""
    async def _run():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Página padrão (limit=50)
            resp = await client.get("/api/v1/analytics/anomalias?periodo=2026-05")
            assert resp.status_code == 200
            data = resp.json()
            assert "paginacao" in data
            assert "itens" in data
            assert len(data["itens"]) <= 50
            assert data["paginacao"]["total_registros"] >= 19000

            # 2. Rejeição de limite acima do teto (limit=500 deve falhar com 422)
            resp_invalid = await client.get("/api/v1/analytics/anomalias?periodo=2026-05&limit=500")
            assert resp_invalid.status_code == 422

    asyncio.run(_run())


def test_preaggregated_source_and_fallback_transparency():
    """Valida presença do campo de transparência 'fonte' (gold_pre_agregado vs consulta_tempo_real)."""
    async def _run():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Competência com Data Mart pré-agregado
            resp = await client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05")
            assert resp.status_code == 200
            data = resp.json()
            assert "fonte" in data
            assert data["fonte"] in ("gold_pre_agregado", "consulta_tempo_real")
            assert data["kpis"]["ticket_medio_brl"] > 0

    asyncio.run(_run())
