"""
QIMED Analytics API - Ponto de entrada FastAPI.
Fornece camada de consulta analítica de alta performance diretamente no DuckDB Gold.
"""
from fastapi import FastAPI
from src.api.routers.analytics import router as analytics_router
from src.api.routers.drilldown import router as drilldown_router
from src.api.routers.uploads import router as uploads_router
from src.api.routers.triggers import router as triggers_router

app = FastAPI(
    title="QIMED Analytics & Ingestion API",
    description="Camada analítica e de ingestão assíncrona do QIMED Lakehouse",
    version="3.2.0",
)
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(drilldown_router, prefix="/api/v1")
app.include_router(uploads_router, prefix="/api/v1")
app.include_router(triggers_router, prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok", "olap_source": "duckdb_gold"}
