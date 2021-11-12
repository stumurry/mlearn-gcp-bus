"""
$dag_filename$: staging_to_warehouse.py
"""

import logging
import os
from libs import GCLOUD as gcloud, parse_template
from airflow import DAG, settings
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure

env = os.environ['ENV']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 3, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def _marker(st):
    logging.info('********************************{}*****************************************'.format(st))


def parse_query(table, **kwargs):
    """
    Airflow is passing additional arguments: conf,
    so we need to deal with them. (kwargs).
    """
    query = parse_template(f'{settings.DAGS_FOLDER}/templates/sql/staging_to_{table}.sql')
    return query


DAG_ID = 'staging_to_warehouse'

table_map = {
    'warehouse.distributor_orders': []
}


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval='@daily',
              catchup=False)
    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(task_id='finish')

        for table, sources in table_map.items():
            pusher_task_id = f'schedule_dataflow_{table}'
            parsed_table = gcloud.parse_table_name(table)
            parse_query_task = PythonOperator(
                task_id=f'parse_query_{table}',
                python_callable=parse_query,
                op_args=[table]
            )

            dataflow_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=gcloud.project(env),
                template_name=f'load_staging_to_warehouse_{parsed_table}',
                job_name=f'staging-to-warehouse-{table}',
                pull_parameters=[{
                    'param_name': 'query',
                    'task_id': f'parse_query_{table}'
                }]
            )

            monitor_dataflow_task = DataflowJobStateSensor(
                task_id=f'monitor_df_job_{table}',
                dag=dag,
                pusher_task_id=pusher_task_id
            )

            start_task.set_downstream(parse_query_task)
            parse_query_task.set_downstream(dataflow_task)
            dataflow_task.set_downstream(monitor_dataflow_task)
            monitor_dataflow_task.set_downstream(finish_task)

        start_task >> finish_task
    return dag


globals()[DAG_ID] = create_dag()
