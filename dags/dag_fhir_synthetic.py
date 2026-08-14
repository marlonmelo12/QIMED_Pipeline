from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'qimed_fhir_synthetic',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['fhir', 'synthetic', 'bronze']
)

def generate_bundles(**kwargs): pass
def parse_resources(**kwargs): pass
def lgpd_gate(**kwargs): pass
def validate(**kwargs): pass
def write_bronze(**kwargs): pass
def register_catalog(**kwargs): pass

t1 = PythonOperator(task_id='generate_bundles', python_callable=generate_bundles, dag=dag)
t2 = PythonOperator(task_id='parse_resources', python_callable=parse_resources, dag=dag)
t3 = PythonOperator(task_id='lgpd_gate', python_callable=lgpd_gate, dag=dag)
t4 = PythonOperator(task_id='validate', python_callable=validate, dag=dag)
t5 = PythonOperator(task_id='write_bronze', python_callable=write_bronze, dag=dag)
t6 = PythonOperator(task_id='register_catalog', python_callable=register_catalog, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
