from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.api.routers.analytics import router as analytics_router
from src.api.routers.drilldown import router as drilldown_router
from src.api.routers.uploads import router as uploads_router
from src.api.routers.triggers import router as triggers_router
from src.metadata.database import engine
from src.metadata.models import Base

from fastapi.middleware.gzip import GZipMiddleware
try:
    import orjson
    from fastapi.responses import ORJSONResponse
    default_resp = ORJSONResponse
except (ImportError, Exception):
    from fastapi.responses import JSONResponse
    default_resp = JSONResponse

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Cria as tabelas do SQLAlchemy ao ligar a API
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield

import time
from fastapi import Request

app = FastAPI(
    title="QIMED Analytics & Ingestion API",
    description="Camada analítica e de ingestão assíncrona do QIMED Lakehouse",
    version="3.2.0",
    default_response_class=default_resp,
    lifespan=lifespan
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time_ms = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.3f}"
    return response

app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(drilldown_router, prefix="/api/v1")
app.include_router(uploads_router, prefix="/api/v1")
app.include_router(triggers_router, prefix="/api/v1")

@app.get("/health")
def health():
    return {"status": "ok", "olap_source": "duckdb_gold"}
