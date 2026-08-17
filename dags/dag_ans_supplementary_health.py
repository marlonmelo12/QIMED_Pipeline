"""
DAG do Apache Airflow para Ingestao Mensal de Dados Abertos da ANS / D-TISS (Saude Suplementar).
Captura beneficiarios, sinistralidade e despesa assistencial privada por municipio.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'qimed_ans_supplementary_health',
    default_args=default_args,
    description='Pipeline de ingestao de dados de saude suplementar, beneficiarios e operadoras da ANS',
    schedule_interval='@monthly',
    catchup=False,
    tags=['ans', 'supplementary_health', 'operadoras', 'bronze', 'silver']
)

def fetch_ans_open_data(**kwargs):
    """Coleta dados abertos de operadoras e beneficiarios da ANS."""
    from src.collectors.ans_collector import AnsCollector
    collector = AnsCollector(uf="AC")
    return collector.fetch()

def validate_ans_data(**kwargs):
    """Valida integridade do esquema e colunas da ANS."""
    pass

def write_bronze_ans(**kwargs):
    """Persiste dados da ANS na Camada Bronze (Delta Lake)."""
    pass

def transform_to_silver_health_plans(**kwargs):
    """Gera dimensao dim_health_plans e calcula taxa de cobertura privada."""
    pass

t1 = PythonOperator(task_id='fetch_ans_open_data', python_callable=fetch_ans_open_data, dag=dag)
t2 = PythonOperator(task_id='validate_ans_data', python_callable=validate_ans_data, dag=dag)
t3 = PythonOperator(task_id='write_bronze_ans', python_callable=write_bronze_ans, dag=dag)
t4 = PythonOperator(task_id='transform_to_silver_health_plans', python_callable=transform_to_silver_health_plans, dag=dag)

t1 >> t2 >> t3 >> t4
