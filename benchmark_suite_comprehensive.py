"""
Benchmark Completo e Rigoroso de Latência (Server vs End-to-End), Throughput e Estabilidade.
Reporta formalmente:
1. Server Latency (tempo interno do servidor medido no gateway ASGI)
2. Client End-to-End Latency (tempo total observado na rede)
3. Cenário A: Warm Cache (100% Hit)
4. Cenário B: Cold Cache & Single-Flight
5. Cenário C: Simulação de Produção (95% Hit / 5% Miss)
6. Cenário D: Carga Concorrente Escalonada (10, 25, 50, 100, 250 threads)
"""
import time
import statistics
import concurrent.futures
import requests
import random

BASE_URL = "http://localhost:8000/api/v1"

ENDPOINTS = [
    ("Dashboard Financeiro", "/analytics/dashboard/financeiro?periodo=2026-05"),
    ("Drilldown Ticket Médio", "/analytics/drilldown/ticket-medio?periodo=2026-05"),
    ("Drilldown Custo Total", "/analytics/drilldown/custo-total?periodo=2026-05"),
    ("Drilldown Custo Desfecho", "/analytics/drilldown/custo-desfecho?periodo=2026-05"),
    ("Central de Anomalias (Grid)", "/analytics/central-anomalias?periodo=2026-05"),
    ("Alertas Forenses Paginados", "/analytics/anomalias?periodo=2026-05&limit=50"),
    ("Hospitais Eficiência (Paginado)", "/analytics/hospitais/eficiencia?periodo=2026-05&uf=SP&limit=50"),
]

def run_benchmark():
    print("=" * 110)
    print("RELATÓRIO TÉCNICO DE BENCHMARK: QIMED HEALTH LAKEHOUSE ANALYTICS API (V3.2)")
    print("Topologia: 4 Workers Uvicorn | Cache L1 Pré-Comprimido GZip | Single-Flight por Chave | orjson")
    print("=" * 110)

    # 1. Warmup completo de todos os 4 workers
    session = requests.Session()
    print("\n[1] WARMUP DE TODOS OS 4 WORKERS UVICORN & VALIDAÇÃO DE HEADERS:")
    for name, path in ENDPOINTS:
        url = f"{BASE_URL}{path}"
        for _ in range(8):  # 8 requests para aquecer todos os 4 workers
            r = session.get(url, headers={"Accept-Encoding": "gzip"})
        cache_hdr = r.headers.get("x-cache", "N/A")
        enc_hdr = r.headers.get("content-encoding", "identity")
        srv_ms = float(r.headers.get("x-process-time-ms", 0.0))
        print(f"  ✓ {name:<35} | Status: {r.status_code} | X-Cache: {cache_hdr:<8} | Encoding: {enc_hdr:<5} | Server: {srv_ms:6.2f}ms | Size: {len(r.content):6d} B")

    # 2. Cenário A - Warm Cache (100 iterações por endpoint)
    print("\n[2] CENÁRIO A: WARM CACHE (100% HIT - 100 requisições por endpoint com HTTP Keep-Alive)")
    print(f"  {'Endpoint':<32} | {'Server P50':<11} | {'Server P95':<11} | {'Client P50':<11} | {'Client P95':<11} | {'SLA Server (<20ms)':<18}")
    print("  " + "-" * 104)

    all_server_times = []
    all_client_times = []

    for name, path in ENDPOINTS:
        url = f"{BASE_URL}{path}"
        server_lats = []
        client_lats = []
        for _ in range(100):
            t0 = time.perf_counter()
            r = session.get(url, headers={"Accept-Encoding": "gzip"})
            c_lat = (time.perf_counter() - t0) * 1000
            s_lat = float(r.headers.get("x-process-time-ms", 0.0))
            server_lats.append(s_lat)
            client_lats.append(c_lat)
            all_server_times.append(s_lat)
            all_client_times.append(c_lat)

        s_p50 = statistics.median(server_lats)
        s_p95 = statistics.quantiles(server_lats, n=100)[94]
        c_p50 = statistics.median(client_lats)
        c_p95 = statistics.quantiles(client_lats, n=100)[94]
        sla_pass = "✓ APROVADO" if s_p95 < 20.0 else "REPROVADO"

        print(f"  {name:<32} | {s_p50:8.2f} ms | {s_p95:8.2f} ms | {c_p50:8.2f} ms | {c_p95:8.2f} ms | {sla_pass:<18}")

    p50_srv_g = statistics.median(all_server_times)
    p95_srv_g = statistics.quantiles(all_server_times, n=100)[94]
    p99_srv_g = statistics.quantiles(all_server_times, n=100)[98]
    print(f"\n  MÉTRICAS GLOBAIS DE SERVIDOR (700 REQS WARM): P50 = {p50_srv_g:.2f}ms | P95 = {p95_srv_g:.2f}ms | P99 = {p99_srv_g:.2f}ms")

    # 3. Cenário B - Cold Cache com Single Flight
    print("\n[3] CENÁRIO B: COLD CACHE & SINGLE-FLIGHT (50 requisições simultâneas em competência aberta)")
    cold_url = f"{BASE_URL}/analytics/dashboard/financeiro?periodo=2026-05&uf=RN"
    def fetch_cold(sess):
        t0 = time.perf_counter()
        r = sess.get(cold_url, headers={"Accept-Encoding": "gzip"})
        s_ms = float(r.headers.get("x-process-time-ms", 0.0))
        return (time.perf_counter() - t0) * 1000, s_ms, r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        t_start = time.perf_counter()
        futures = [executor.submit(fetch_cold, requests.Session()) for _ in range(50)]
        results = [f.result() for f in futures]
        total_dur = time.perf_counter() - t_start

    cold_server_times = [r[1] for r in results]
    cold_errors = sum(1 for r in results if r[2] != 200)
    print(f"  50 Reqs Concorrentes Cold: Tempo Total = {total_dur*1000:.1f}ms | Server P50 = {statistics.median(cold_server_times):.1f}ms | Erros = {cold_errors}")

    # 4. Cenário C - Produção Simulada (95% Hit / 5% Miss)
    print("\n[4] CENÁRIO C: PRODUÇÃO SIMULADA (95% HIT / 5% MISS - 200 Requisições)")
    mix_urls = []
    for _ in range(190):  # 95% Hit
        mix_urls.append(f"{BASE_URL}/analytics/dashboard/financeiro?periodo=2026-05")
    for i in range(10):   # 5% Miss
        mix_urls.append(f"{BASE_URL}/analytics/dashboard/financeiro?periodo=2026-05&uf=UF_MISS_{i}")
    
    random.shuffle(mix_urls)
    mix_server_times = []
    for u in mix_urls:
        r = session.get(u, headers={"Accept-Encoding": "gzip"})
        mix_server_times.append(float(r.headers.get("x-process-time-ms", 0.0)))
    
    print(f"  Mix 95/5: Server P50 = {statistics.median(mix_server_times):.2f}ms | Server P95 = {statistics.quantiles(mix_server_times, n=100)[94]:.2f}ms | Server P99 = {statistics.quantiles(mix_server_times, n=100)[98]:.2f}ms")

    # 5. Cenário D - Carga Concorrente Escalonada (Throughput)
    print("\n[5] CENÁRIO D: TESTE DE CARGA CONCORRENTE ESCALONADA (THROUGHPUT & CONCURRENCY)")
    print(f"  {'Concorrência':<15} | {'Total Reqs':<12} | {'Tempo Total':<12} | {'Throughput (RPS)':<18} | {'Server P95':<12} | {'Erros':<8}")
    print("  " + "-" * 90)

    concurrency_levels = [10, 25, 50, 100, 250]
    for concurrency in concurrency_levels:
        req_count = concurrency * 4
        urls = [f"{BASE_URL}{ENDPOINTS[i % len(ENDPOINTS)][1]}" for i in range(req_count)]

        def worker_task(sub_urls):
            sess = requests.Session()
            lats = []
            s_lats = []
            errs = 0
            for u in sub_urls:
                try:
                    r = sess.get(u, headers={"Accept-Encoding": "gzip"}, timeout=5.0)
                    s_lats.append(float(r.headers.get("x-process-time-ms", 0.0)))
                    lats.append(1)
                    if r.status_code != 200:
                        errs += 1
                except Exception:
                    errs += 1
            return lats, s_lats, errs

        chunk_size = max(1, len(urls) // concurrency)
        chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

        t_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker_task, ch) for ch in chunks]
            completed = [f.result() for f in futures]
        t_total = time.perf_counter() - t_start

        all_reqs = [r for c in completed for r in c[0]]
        all_s_lats = [s for c in completed for s in c[1]]
        total_errs = sum(c[2] for c in completed)
        rps = len(all_reqs) / t_total if t_total > 0 else 0
        p95_s = statistics.quantiles(all_s_lats, n=100)[94] if len(all_s_lats) >= 100 else (statistics.median(all_s_lats) if all_s_lats else 0)

        print(f"  {concurrency:<15} | {len(all_reqs):<12} | {t_total:8.3f} s   | {rps:14.1f} req/s | {p95_s:8.2f} ms | {total_errs:<8}")

    print("=" * 110)

if __name__ == "__main__":
    run_benchmark()
