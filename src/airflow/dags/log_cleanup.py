from airflow import DAG, settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from libs import report_failure
from libs import GCLOUD as gcloud, CloudStorage
import logging
import os
import re
from datetime import datetime, timedelta


DAG_ID = 'log_cleanup'
DAGS_FOLDER = CloudStorage.remove_prefix(settings.DAGS_FOLDER, 'gs://')
env = os.environ['ENV']
project_id = gcloud.project(env)
log = logging.getLogger()
airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
date_now = datetime.now().strftime('%Y-%m-%d')
days_past = '30'
default_args = {
    'owner': 'airflow',
    'description': 'Clears old log files after a certain date',
    'start_date': datetime(2020, 6, 20),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def find_log_blobs(blobs):
    pattern = r'\d{4}[-]\d{2}[-]\d{2}.+\.log'
    group = r'\d{4}[-]\d{2}[-]\d{2}'
    for blob in blobs:
        if re.search(pattern, blob.name):
            g = re.search(group, blob.name).group()
            yield (datetime.strptime(g, '%Y-%m-%d').date(), blob)


def filter_log_blobs_by_days_past(blobs, date_now, days_past):
    past_date = date_now.date() - timedelta(days=days_past)
    for d in list(find_log_blobs(blobs)):
        if d[0] <= past_date:
            yield d[1]


def clear_log_files_by_bucket(bucket_name, date_now, days_past):
    date_now = datetime.strptime(date_now, '%Y-%m-%d')
    days_past = int(days_past)
    storage = CloudStorage.factory(project_id)
    blobs = storage.list_blobs(bucket_name)
    for blob in filter_log_blobs_by_days_past(blobs, date_now, days_past):
        blob.delete()


def create_dag():
    log.info(f'dag_folder = {DAGS_FOLDER}')
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval='@monthly',  # decide on an interval
              catchup=False)
    with dag:
        start_task = DummyOperator(
            task_id='start'
        )

        finish_task = DummyOperator(
            task_id='finish'
        )

        clear_log_files_task = PythonOperator(
            task_id='clear_log_files_task',
            python_callable=clear_log_files_by_bucket,
            op_args=[DAGS_FOLDER, date_now, days_past]
        )

        (
            start_task
            >> clear_log_files_task
            >> finish_task
        )
    return dag


globals()[DAG_ID] = create_dag()
