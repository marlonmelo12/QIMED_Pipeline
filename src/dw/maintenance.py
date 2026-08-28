"""
Módulo de Manutenção e Compactação Física do Storage DuckDB - QIMED Lakehouse V3.
Executa otimizações de disco, expurgo de páginas desalocadas e manutenção periódica com retries.
"""
import os
import time
import logging
import duckdb

logger = logging.getLogger("QIMED_MAINTENANCE")


def otimizar_storage_duckdb(
    db_path: str = "warehouse/qimed_silver_completa.duckdb",
    max_retries: int = 3,
    initial_delay: float = 0.5
):
    """
    Executa CHECKPOINT e VACUUM para expurgo de blocos mortos e compactação de disco.
    Aplica política de retries com backoff exponencial para contornar contenção transitória de lock.
    """
    if not os.path.exists(db_path):
        logger.warning(f"Arquivo de banco de dados não encontrado: {db_path}")
        return

    logger.info(f"Iniciando manutenção e compactação em {db_path}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            conn = duckdb.connect(db_path)
            try:
                conn.execute("CHECKPOINT;")
                conn.execute("VACUUM;")
                logger.info(f"Manutenção de storage concluída com sucesso em {db_path} na tentativa {attempt}.")
                return
            finally:
                conn.close()
        except Exception as e:
            if attempt < max_retries:
                delay = initial_delay * (2 ** (attempt - 1))
                logger.warning(f"Tentativa {attempt} de CHECKPOINT/VACUUM falhou em {db_path} ({e}). Tentando novamente em {delay:.1f}s...")
                time.sleep(delay)
            else:
                logger.error(f"Falha definitiva ao executar manutenção em {db_path} após {max_retries} tentativas: {e}", exc_info=True)
                raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for db in ["warehouse/qimed_silver_completa.duckdb", "warehouse/qimed_dw.duckdb"]:
        otimizar_storage_duckdb(db)
