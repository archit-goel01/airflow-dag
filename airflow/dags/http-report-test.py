from datetime import datetime
import json
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(dag_id='http_springboot_rest',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['http-rest'],
         catchup=False) as dag:
    execute_rest = SimpleHttpOperator(
        task_id='report-endpoint',
        http_conn_id='rs-report-hostname',
        endpoint='stvs/v1/rs-reports-generator/v1/execute/job/uk/rs/report',
        method='GET',
        headers={"Content-Type": "application/json"},
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

execute_rest