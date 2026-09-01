from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.api.routers.analytics import router as analytics_router
from src.api.routers.drilldown import router as drilldown_router
from src.api.routers.uploads import router as uploads_router
from src.api.routers.triggers import router as triggers_router
from src.metadata.database import engine
from src.metadata.models import Base

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cria as tabelas do SQLAlchemy ao ligar a API
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(
    title="QIMED Analytics & Ingestion API",
    description="Camada analítica e de ingestão assíncrona do QIMED Lakehouse",
    version="3.2.0",
    lifespan=lifespan
)
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(drilldown_router, prefix="/api/v1")
app.include_router(uploads_router, prefix="/api/v1")
app.include_router(triggers_router, prefix="/api/v1")

@app.get("/health")
def health():
    return {"status": "ok", "olap_source": "duckdb_gold"}
