import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# Configuração Padrão do Control Plane
DB_URL = os.getenv(
    "QIMED_DATABASE_URL", 
    "postgresql+asyncpg://qimed:qimed_secret@localhost:5433/qimed"
)

# engine com pooling
engine = create_async_engine(DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=10)
AsyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)

Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

