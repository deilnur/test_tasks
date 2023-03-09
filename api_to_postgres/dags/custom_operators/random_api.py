from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import AirflowException
import pandas as pd
import requests
import datetime

class RandomApiToPostgres(BaseOperator):
    """
    Args:
        conn_id: connction string for Postgres ,
        rows_count: count of rows to get from api and insert to Postgres, default = 100
    """
    
    def __init__(self, conn_id, rows_count = 100, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.rows_count = rows_count

    def execute(self, context):
        base_url = "https://random-data-api.com/api/cannabis/random_cannabis"
        size_per_request = 100
        rows_data = []
        params = {'size': size_per_request, 'response_type': 'json'}
        while len(rows_data) < self.rows_count:
            res = requests.get(base_url, params = params)
            if res.status_code == 200:
                self.log.info(f"Succes, code = {res.status_code}")
                res_list = res.json()
                rows_data.extend(res_list)
            else:
                raise AirflowException(res.text)
        if len(rows_data) > 0:
            res_df = pd.DataFrame.from_records(res_list)
            res_df['processed_dttm'] = datetime.datetime.now()
            postgres_hook = PostgresHook(self.conn_id)
            engine = postgres_hook.get_sqlalchemy_engine()
            res_df.to_sql("random_cannabis", engine, if_exists = 'append', index = False)
            self.log.info(f"{len(rows_data)} rows was inserted")
