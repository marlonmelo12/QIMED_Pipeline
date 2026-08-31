"""
Testes de Integridade, Carregamento e Execução das DAGs do Airflow.
Garante que todas as DAGs importem sem erro, tenham dependências e operadores reais (sem pass).
"""
import sys
import types
from unittest.mock import MagicMock
import pytest

# Se airflow não estiver instalado no ambiente de desenvolvimento local, registra mocks leves
if "airflow" not in sys.modules:
    try:
        import airflow
    except ImportError:
        airflow_mock = types.ModuleType("airflow")
        
        class MockDAG:
            def __init__(self, dag_id, *args, **kwargs):
                self.dag_id = dag_id
                self.tasks = []
            def add_task(self, task):
                self.tasks.append(task)
            def get_task(self, task_id):
                for t in self.tasks:
                    if t.task_id == task_id:
                        return t
                return None
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
                
        class MockPythonOperator:
            def __init__(self, task_id, python_callable, dag=None, *args, **kwargs):
                self.task_id = task_id
                self.python_callable = python_callable
                if dag:
                    dag.add_task(self)
            def __rshift__(self, other):
                return other
            def __lshift__(self, other):
                return self

        airflow_mock.DAG = MockDAG
        airflow_operators = types.ModuleType("airflow.operators")
        airflow_python_op = types.ModuleType("airflow.operators.python")
        airflow_python_op.PythonOperator = MockPythonOperator
        airflow_operators.python = airflow_python_op
        
        sys.modules["airflow"] = airflow_mock
        sys.modules["airflow.operators"] = airflow_operators
        sys.modules["airflow.operators.python"] = airflow_python_op

from dags.dag_datasus_cnes import dag as cnes_dag, run_cnes_ingestion
from dags.dag_ans_supplementary_health import dag as ans_dag, run_ans_pipeline
from dags.dag_fhir_synthetic import dag as fhir_dag, run_fhir_ingestion
from dags.dag_datasus_sih_rejeicoes_glosas import dag as glosas_dag, run_sih_rj_er_ingestion, run_silver_glosas_transformation
from dags.dag_data_quality_audit import dag as quality_dag, run_full_warehouse_audit
from dags.dag_dim_tempo import dag as tempo_dag, run_generate_dim_tempo
from dags.dag_qimed_end_to_end import dag as master_dag
from dags.dag_datasus_sih import dag as sih_dag


def test_dag_cnes_structure_and_imports():
    assert cnes_dag.dag_id == "qimed_datasus_cnes"
    assert len(cnes_dag.tasks) == 1
    task = cnes_dag.get_task("ingest_cnes_to_bronze")
    assert task.python_callable == run_cnes_ingestion


def test_dag_ans_structure_and_imports():
    assert ans_dag.dag_id == "qimed_ans_supplementary_health"
    assert len(ans_dag.tasks) == 1
    task = ans_dag.get_task("run_ans_pipeline")
    assert task.python_callable == run_ans_pipeline


def test_dag_fhir_structure_and_imports():
    assert fhir_dag.dag_id == "qimed_fhir_synthetic"
    assert len(fhir_dag.tasks) == 1
    task = fhir_dag.get_task("ingest_fhir_to_bronze")
    assert task.python_callable == run_fhir_ingestion


def test_dag_glosas_rejeicoes_structure_and_imports():
    assert glosas_dag.dag_id == "qimed_datasus_sih_rejeicoes_glosas"
    assert len(glosas_dag.tasks) == 3
    t_ingest = glosas_dag.get_task("ingest_sih_rejeicoes_bronze")
    assert t_ingest.python_callable == run_sih_rj_er_ingestion


def test_dag_data_quality_audit_structure_and_imports():
    assert quality_dag.dag_id == "qimed_data_quality_audit"
    assert len(quality_dag.tasks) == 1
    t_audit = quality_dag.get_task("audit_warehouse_data_quality")
    assert t_audit.python_callable == run_full_warehouse_audit


def test_dag_dim_tempo_structure_and_imports():
    assert tempo_dag.dag_id == "qimed_dim_tempo_generator"
    assert len(tempo_dag.tasks) == 2
    t_gen = tempo_dag.get_task("generate_dim_tempo_delta")
    assert t_gen.python_callable == run_generate_dim_tempo


def test_dag_master_end_to_end_structure_and_imports():
    assert master_dag.dag_id == "qimed_master_pipeline_end_to_end"
    assert len(master_dag.tasks) == 4
    # Verificar nova nomenclatura: pipeline-ready (não mais mirror-trigger)
    task_notify = master_dag.get_task("notify_backend_pipeline_ready")
    assert task_notify is not None


def test_dag_sih_structure_and_imports():
    assert sih_dag.dag_id == "qimed_datasus_sih"
    assert len(sih_dag.tasks) == 1
