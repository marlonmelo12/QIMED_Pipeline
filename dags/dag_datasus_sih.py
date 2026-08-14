from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'qimed',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'qimed_datasus_sih',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['datasus', 'sih', 'bronze']
)

def check_ftp_availability(**kwargs): pass
def download_sih(**kwargs): pass
def decompress(**kwargs): pass
def parse(**kwargs): pass
def lgpd_gate(**kwargs): pass
def validate(**kwargs): pass
def write_bronze(**kwargs): pass
def register_catalog(**kwargs): pass

t1 = PythonOperator(task_id='check_ftp_availability', python_callable=check_ftp_availability, dag=dag)
t2 = PythonOperator(task_id='download_sih', python_callable=download_sih, dag=dag)
t3 = PythonOperator(task_id='decompress', python_callable=decompress, dag=dag)
t4 = PythonOperator(task_id='parse', python_callable=parse, dag=dag)
t5 = PythonOperator(task_id='lgpd_gate', python_callable=lgpd_gate, dag=dag)
t6 = PythonOperator(task_id='validate', python_callable=validate, dag=dag)
t7 = PythonOperator(task_id='write_bronze', python_callable=write_bronze, dag=dag)
t8 = PythonOperator(task_id='register_catalog', python_callable=register_catalog, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
