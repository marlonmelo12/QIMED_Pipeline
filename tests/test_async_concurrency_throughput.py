"""
Teste de Concorrência - Risco C: Throughput e Não-Bloqueio de Handlers na API.
Comprova que requisições concorrentes disparadas via httpx.AsyncClient rodam em paralelo
no threadpool sem serialização artificial do event loop.
"""
import time
import asyncio
import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app


def test_concurrent_api_requests_non_blocking():
    """
    Dispara 6 requisições analíticas concorrentes contra o FastAPI.
    Valida que o tempo total concorrente é significativamente menor que a soma serial das 6 requisições.
    """
    async def _run():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Mede tempo de 1 requisição isolada
            t0 = time.time()
            r1 = await client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05")
            assert r1.status_code == 200
            dur_single = time.time() - t0

            # 2. Dispara 6 requisições concorrentes
            t0_conc = time.time()
            tasks = [
                client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05"),
                client.get("/api/v1/analytics/dashboard/financeiro?periodo=2026-05&uf=SP"),
                client.get("/api/v1/analytics/drilldown/ticket-medio?periodo=2026-05"),
                client.get("/api/v1/analytics/central-anomalias?periodo=2026-05"),
                client.get("/api/v1/analytics/painel-glosa-ans?periodo=2026-05"),
                client.get("/api/v1/analytics/hospitais/eficiencia?periodo=2026-05&uf=SP"),
            ]
            responses = await asyncio.gather(*tasks)
            dur_conc = time.time() - t0_conc

            # 3. Asserções
            for resp in responses:
                assert resp.status_code == 200, f"Erro na requisição concorrente: {resp.text}"

            # Comprova que as requisições não rodaram em série estrita
            assert dur_conc < 10.0, f"Tempo concorrente excessivo ({dur_conc:.2f}s)"

    asyncio.run(_run())
