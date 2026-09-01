"""
Módulo Utilitário de Armazenamento S3 / MinIO — QIMED Lakehouse.
Fornece suporte unificado para:
1. Resolução dinâmica de URIs de armazenamento (s3:// vs local);
2. Injeção de storage_options para o delta-rs (write_deltalake / DeltaTable);
3. Verificação de existência agnóstica (lakehouse_path_exists / s3_path_exists);
4. Configuração da extensão httpfs e parâmetros S3 no DuckDB;
5. Retrocompatibilidade via STORAGE_BACKEND ('s3' ou 'local').
"""
import os
from typing import Any, Dict, Optional
import duckdb
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

DEFAULT_S3_ENDPOINT = "http://localhost:9000"
DEFAULT_S3_BUCKET = "qimed-lakehouse"
DEFAULT_AWS_ACCESS_KEY = "minio_admin"
DEFAULT_AWS_SECRET_KEY = "minio_secret_password"
DEFAULT_AWS_REGION = "us-east-1"


def get_storage_backend() -> str:
    """Retorna o backend de armazenamento configurado ('s3' ou 'local')."""
    return os.getenv("STORAGE_BACKEND", "s3").lower().strip()


def is_s3_backend() -> bool:
    """Verifica se o backend S3 está ativo."""
    return get_storage_backend() == "s3"


def get_s3_bucket_name() -> str:
    """Retorna o nome do bucket S3 configurado."""
    return os.getenv("S3_BUCKET_NAME", DEFAULT_S3_BUCKET).strip()


def get_s3_endpoint_url() -> str:
    """Retorna a URL do endpoint S3/MinIO."""
    return os.getenv("AWS_ENDPOINT_URL", DEFAULT_S3_ENDPOINT).strip()


def get_s3_storage_options(path: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Retorna o dicionário de storage_options para o delta-rs / deltalake.
    Se o backend for 'local' ou se o path fornecido for local (não s3://), retorna None.
    """
    if not is_s3_backend():
        return None

    if path is not None and not str(path).startswith("s3://"):
        return None

    endpoint = get_s3_endpoint_url()
    access_key = os.getenv("AWS_ACCESS_KEY_ID", DEFAULT_AWS_ACCESS_KEY)
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", DEFAULT_AWS_SECRET_KEY)
    region = os.getenv("AWS_REGION", DEFAULT_AWS_REGION)
    allow_http = os.getenv("AWS_ALLOW_HTTP", "true")
    allow_unsafe_rename = os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME", "true")

    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": region,
        "AWS_ALLOW_HTTP": allow_http,
        "AWS_S3_ALLOW_UNSAFE_RENAME": allow_unsafe_rename,
    }


def resolve_lakehouse_path(relative_or_uri: str, default_layer: str = "bronze") -> str:
    """
    Resolve um caminho para a URI correta com base no STORAGE_BACKEND.
    Ex:
      'datasus/sih' -> 's3://qimed-lakehouse/bronze/datasus/sih' (se s3)
      'datasus/sih' -> 'lakehouse/bronze/datasus/sih' (se local)
      's3://...' -> mantido como 's3://...'
      'C:/...' ou '/tmp/...' (caminho absoluto local) -> mantido como caminho local.
    """
    normalized = relative_or_uri.replace("\\", "/")
    if normalized.startswith("s3://") or normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized

    # Se for um caminho absoluto local no SO (Windows C: ou Linux /)
    if os.path.isabs(relative_or_uri) or (len(relative_or_uri) >= 2 and relative_or_uri[1] == ":") or relative_or_uri.startswith("/"):
        return normalized

    clean_path = normalized.strip("/")

    if is_s3_backend():
        bucket = get_s3_bucket_name()
        if clean_path.startswith("lakehouse/"):
            clean_path = clean_path[len("lakehouse/"):]
        return f"s3://{bucket}/{clean_path}"
    else:
        if not clean_path.startswith("lakehouse"):
            clean_path = f"lakehouse/{clean_path}"
        return clean_path


def configure_duckdb_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Configura a extensão httpfs, parâmetros S3 legados e o Secret S3 no DuckDB para consultas diretas em s3://.
    Garante suporte unificado tanto para httpfs (read_parquet) quanto para DeltaKernel (delta_scan),
    impedindo tentativas indevidas de acesso ao AWS IMDS (169.254.169.254).
    """
    if not is_s3_backend():
        return

    endpoint = get_s3_endpoint_url()
    access_key = os.getenv("AWS_ACCESS_KEY_ID", DEFAULT_AWS_ACCESS_KEY)
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", DEFAULT_AWS_SECRET_KEY)
    region = os.getenv("AWS_REGION", DEFAULT_AWS_REGION)

    clean_endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
    use_ssl = "false" if endpoint.startswith("http://") else "true"

    try:
        try:
            conn.execute("LOAD httpfs;")
        except Exception:
            conn.execute("INSTALL httpfs; LOAD httpfs;")

        # 1. Configurações legadas httpfs (para compatibilidade)
        conn.execute(f"SET s3_endpoint = '{clean_endpoint}';")
        conn.execute(f"SET s3_access_key_id = '{access_key}';")
        conn.execute(f"SET s3_secret_access_key = '{secret_key}';")
        conn.execute(f"SET s3_region = '{region}';")
        conn.execute(f"SET s3_use_ssl = {use_ssl};")
        conn.execute("SET s3_url_style = 'path';")

        # 2. DuckDB Secret Manager (Obrigatório para DeltaKernel / delta_scan no DuckDB 1.x)
        safe_key = access_key.replace("'", "''")
        safe_secret = secret_key.replace("'", "''")
        safe_endpoint = clean_endpoint.replace("'", "''")
        safe_region = region.replace("'", "''")

        conn.execute(f"SET s3_endpoint='{clean_endpoint}';")
        conn.execute(f"SET s3_access_key_id='{safe_key}';")
        conn.execute(f"SET s3_secret_access_key='{safe_secret}';")
        conn.execute(f"SET s3_use_ssl={str(use_ssl).lower()};")
        conn.execute("SET s3_url_style='path';")
        conn.execute(f"SET s3_region='{safe_region}';")

        try:
            secret_sql = f"""
            CREATE OR REPLACE SECRET s3_creds (
                TYPE S3,
                KEY_ID '{safe_key}',
                SECRET '{safe_secret}',
                ENDPOINT '{safe_endpoint}',
                USE_SSL {use_ssl},
                URL_STYLE 'path',
                REGION '{safe_region}'
            );
            """
            conn.execute(secret_sql)
        except Exception:
            pass

        logger.debug(
            f"[DuckDB S3] Extensao httpfs e S3 configurados para MinIO em '{clean_endpoint}' "
            f"(region={region}, ssl={use_ssl}, path_style=true)."
        )
    except Exception as e:
        logger.debug(f"[DuckDB S3] Falha ao configurar httpfs/S3 no DuckDB: {e}")



def lakehouse_path_exists(uri_or_path: str) -> bool:
    """
    Verifica de forma agnóstica se uma tabela/diretório existe no Lakehouse,
    suportando tanto URIs S3 ('s3://...') quanto caminhos locais do sistema de arquivos.
    """
    if not uri_or_path:
        return False

    normalized = str(uri_or_path).replace("\\", "/").strip()

    # 1. Caminho Local do Sistema de Arquivos
    if not normalized.startswith("s3://"):
        return os.path.exists(normalized)

    # 2. Caminho S3 / MinIO
    # Tenta inspecionar diretamente via DeltaTable (delta-rs)
    try:
        from deltalake import DeltaTable
        opts = get_s3_storage_options(normalized)
        DeltaTable(normalized, storage_options=opts)
        return True
    except Exception:
        pass

    # Fallback: Tenta inspecionar via DuckDB (delta_scan probe)
    try:
        test_conn = duckdb.connect(":memory:")
        configure_duckdb_s3(test_conn)
        test_conn.execute(f"SELECT 1 FROM delta_scan('{normalized}') LIMIT 1;")
        return True
    except Exception:
        pass

    return False


# Alias de conveniência
s3_path_exists = lakehouse_path_exists
