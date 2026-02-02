import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'dhias renaldy',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('P2M3_013_RMT_patient_etl',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:

    python_extract = BashOperator(task_id='python_extract', bash_command='sudo -u airflow python /opt/airflow/scripts/P2M3_dhias_extract.py')
    python_transform = BashOperator(task_id='python_transform', bash_command='sudo -u airflow python /opt/airflow/scripts/P2M3_dhias_transform.py')
    python_load = BashOperator(task_id='python_load', bash_command='sudo -u airflow python /opt/airflow/scripts/P2M3_dhias_load.py')
    

python_extract >> python_transform >> python_load