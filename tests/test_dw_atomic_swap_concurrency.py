"""
Teste de Concorrência - Risco A: Build-Then-Swap Atômico no Data Warehouse DuckDB.
Comprova que leituras concorrentes em loop não sofrem de lock ou erro durante o swap atômico de arquivo.
"""
import os
import time
import threading
import duckdb
import pytest


def test_atomic_swap_under_concurrent_readers(tmp_path):
    """
    Simula múltiplos leitores executando queries em loop contínuo enquanto
    o processo de rebuild cria um novo banco e executa os.replace() atômico.
    """
    prod_db = os.path.join(tmp_path, "qimed_dw_prod.duckdb")

    # 1. Cria o banco de produção inicial com versão 1
    conn = duckdb.connect(prod_db)
    conn.execute("CREATE TABLE status_dw AS SELECT 1 AS versao, 'versao_inicial' AS tag")
    conn.execute("CREATE TABLE agg_internacoes_uf AS SELECT 'SP' AS uf, 100 AS total_internacoes")
    conn.close()

    stop_event = threading.Event()
    reader_errors = []
    reader_success_counts = [0, 0, 0]

    def reader_worker(worker_id: int):
        while not stop_event.is_set():
            try:
                # Cada request da API abre uma conexão efêmera read_only
                with duckdb.connect(prod_db, read_only=True) as read_conn:
                    row = read_conn.execute("SELECT versao, tag FROM status_dw;").fetchone()
                    assert row is not None
                    assert row[0] in (1, 2, 3)
                    cnt = read_conn.execute("SELECT COUNT(*) FROM agg_internacoes_uf;").fetchone()[0]
                    assert cnt > 0
                    reader_success_counts[worker_id] += 1
            except Exception as e:
                reader_errors.append((worker_id, str(e)))
            time.sleep(0.002)

    # Inicia 3 threads leitoras concorrentes
    threads = [threading.Thread(target=reader_worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()

    # 2. Executa múltiplos swaps atômicos enquanto as leituras estão ocorrendo
    try:
        for new_version in [2, 3]:
            time.sleep(0.05)
            building_db = os.path.join(tmp_path, f"qimed_dw_building_v{new_version}.duckdb")
            
            # Constrói o novo banco isoladamente
            b_conn = duckdb.connect(building_db)
            b_conn.execute(f"CREATE TABLE status_dw AS SELECT {new_version} AS versao, 'versao_{new_version}' AS tag")
            b_conn.execute(f"CREATE TABLE agg_internacoes_uf AS SELECT 'SP' AS uf, {new_version * 100} AS total_internacoes")
            b_conn.close()

            # Swap atômico
            os.replace(building_db, prod_db)
            time.sleep(0.05)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2.0)

    # 3. Asserções de Segurança e Concorrência: ZERO erros de lock durante toda a janela de swap
    assert len(reader_errors) == 0, f"Erros observados durante o swap concorrente: {reader_errors}"
    assert all(count >= 5 for count in reader_success_counts), f"Contagem de leituras: {reader_success_counts}"

    # Valida estado final do banco
    with duckdb.connect(prod_db, read_only=True) as final_conn:
        final_row = final_conn.execute("SELECT versao, tag FROM status_dw;").fetchone()
        assert final_row[0] == 3
