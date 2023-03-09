import airflow
from airflow import DAG
import datetime
from custom_operators.random_api import RandomApiToPostgres
from datetime import timedelta

with DAG(
    dag_id = 'RandomApiToPostgres_example',
    start_date = datetime.datetime(2023,1,1),
    schedule_interval = timedelta(hours=12),
    catchup = False
    ) as dag:

    t1 = RandomApiToPostgres(
        task_id = 't1',
        conn_id = 'postgres',
        rows_count = 1000
    )
    
    t1