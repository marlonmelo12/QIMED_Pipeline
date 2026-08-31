import httpx
import os
from typing import Dict, Any

class AirflowClient:
    def __init__(self):
        self.base_url = os.getenv("AIRFLOW_API_URL", "http://localhost:8088/api/v1")
        self.username = os.getenv("AIRFLOW_API_USER", "admin")
        self.password = os.getenv("AIRFLOW_API_PASSWORD", "admin")

    async def trigger_dag(self, dag_id: str, dag_run_id: str, conf: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger an Airflow DAG with deterministic dag_run_id and conf."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/dags/{dag_id}/dagRuns",
                json={
                    "dag_run_id": dag_run_id,
                    "conf": conf
                },
                auth=(self.username, self.password),
                timeout=10.0
            )
            response.raise_for_status()
            return response.json()

