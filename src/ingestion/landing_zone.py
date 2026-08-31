"""
Landing Zone Entry Gate - QIMED Lakehouse.
Responsável pelo recebimento de arquivos, cálculo de hash SHA-256 em streaming,
controle estrito de idempotência no PostgreSQL Control Plane (Regra 19) e persistência na Landing Zone.
"""
import os
import io
import uuid
import hashlib
from typing import Union, BinaryIO, Optional, Dict, Tuple
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.metadata.models import UploadMetadata, JobStatus
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class LandingZoneManager:
    """
    Portão de entrada da Landing Zone com idempotência persistida no Control Plane (PostgreSQL).
    """

    def __init__(self, landing_dir: Optional[str] = None):
        self.landing_dir = landing_dir or os.getenv("LANDING_ZONE_PATH", "lakehouse/landing/uploads")
        os.makedirs(self.landing_dir, exist_ok=True)
        # Fallback in-memory cache para testes unitários isolados sem DB
        self._local_cache_by_hash: Dict[str, UploadMetadata] = {}
        self._local_cache_by_id: Dict[str, UploadMetadata] = {}

    def compute_hash_and_size(
        self,
        file_data: Union[bytes, BinaryIO],
        chunk_size: int = 65536
    ) -> Tuple[str, int]:
        """
        Calcula o hash SHA-256 e o tamanho total em bytes em streaming/chunks (64KB),
        sem carregar o arquivo inteiro na memória (evita OOM para arquivos de até 250MB).
        """
        hasher = hashlib.sha256()
        total_size = 0

        if isinstance(file_data, bytes):
            for i in range(0, len(file_data), chunk_size):
                chunk = file_data[i:i + chunk_size]
                hasher.update(chunk)
                total_size += len(chunk)
        elif hasattr(file_data, "read"):
            if hasattr(file_data, "seek"):
                try:
                    file_data.seek(0)
                except Exception:
                    pass
            while True:
                chunk = file_data.read(chunk_size)
                if not chunk:
                    break
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                hasher.update(chunk)
                total_size += len(chunk)
            if hasattr(file_data, "seek"):
                try:
                    file_data.seek(0)
                except Exception:
                    pass
        else:
            raise ValueError(f"Tipo de arquivo não suportado: {type(file_data)}")

        return hasher.hexdigest(), total_size

    async def get_upload_by_hash(
        self,
        content_hash: str,
        db: Optional[AsyncSession] = None
    ) -> Optional[UploadMetadata]:
        """
        Consulta o registro de uploads no PostgreSQL Control Plane por content_hash.
        """
        if db is not None:
            stmt = select(UploadMetadata).where(UploadMetadata.content_hash == content_hash)
            result = await db.execute(stmt)
            existing = result.scalars().first()
            if existing is not None:
                return existing
        return self._local_cache_by_hash.get(content_hash)

    async def is_duplicate(
        self,
        content_hash: str,
        db: Optional[AsyncSession] = None
    ) -> bool:
        """
        Verifica no PostgreSQL Control Plane se o arquivo com este hash já foi registrado.
        """
        upload = await self.get_upload_by_hash(content_hash, db=db)
        return upload is not None

    def get_upload_by_id(self, upload_id: str) -> Optional[UploadMetadata]:
        """
        Recupera metadados de upload por upload_id a partir do cache local.
        """
        return self._local_cache_by_id.get(upload_id)

    async def save_upload(
        self,
        file_data: Union[bytes, BinaryIO],
        filename: str,
        db: Optional[AsyncSession] = None,
        source: str = "http_upload"
    ) -> UploadMetadata:
        """
        Salva o arquivo na Landing Zone de forma estritamente idempotente.
        1. Calcula SHA-256 via streaming em chunks;
        2. Consulta o PostgreSQL Control Plane para verificar duplicidade;
        3. Se duplicado, retorna o registro persistido anterior sem duplicar disco/bronze;
        4. Se inédito, persiste em lakehouse/landing/uploads/{upload_id}_{filename} e registra no DB.
        """
        content_hash, size_bytes = self.compute_hash_and_size(file_data)

        # 2 & 3. Verificação no Control Plane
        existing = await self.get_upload_by_hash(content_hash, db=db)
        if existing is not None:
            logger.info(
                f"[LANDING ZONE] Arquivo duplicado identificado via PostgreSQL ('{filename}', hash={content_hash[:8]}...). "
                f"Retornando metadados existentes upload_id={existing.upload_id} sem duplicar persistência."
            )
            return existing

        upload_id = f"upl-{uuid.uuid4().hex[:8]}"
        destination_filename = f"{upload_id}_{filename}"
        file_path = os.path.join(self.landing_dir, destination_filename)

        # 4. Gravação física em chunks (streaming)
        chunk_size = 65536
        with open(file_path, "wb") as out_f:
            if isinstance(file_data, bytes):
                for i in range(0, len(file_data), chunk_size):
                    out_f.write(file_data[i:i + chunk_size])
            elif hasattr(file_data, "read"):
                if hasattr(file_data, "seek"):
                    try:
                        file_data.seek(0)
                    except Exception:
                        pass
                while True:
                    chunk = file_data.read(chunk_size)
                    if not chunk:
                        break
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8")
                    out_f.write(chunk)
                if hasattr(file_data, "seek"):
                    try:
                        file_data.seek(0)
                    except Exception:
                        pass

        metadata = UploadMetadata(
            upload_id=upload_id,
            filename=filename,
            content_hash=content_hash,
            size_bytes=size_bytes,
            source=source,
            status=JobStatus.PENDING
        )

        if db is not None:
            db.add(metadata)
        else:
            self._local_cache_by_hash[content_hash] = metadata
            self._local_cache_by_id[upload_id] = metadata

        logger.info(
            f"[LANDING ZONE] Novo upload registrado no Control Plane: {file_path} "
            f"(upload_id={upload_id}, tamanho={size_bytes} bytes, hash={content_hash[:8]}...)"
        )
        return metadata

