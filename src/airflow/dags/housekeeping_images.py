from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import logging
from datetime import datetime, timedelta
from libs import GCLOUD as gcloud
from libs import report_failure
from libs.cleanup import cleanup_xcom

log = logging.getLogger()
log.setLevel('INFO')
env = os.environ['ENV']
project_id = gcloud.project(env)
DAG_ID = 'housekeeping_images'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}
images_list = [
  'airflow',
  'base',
  'cicd-airflow',
  'cicd-dataflow',
  'cicd-workspace',
  'dataflow',
  'githooks',
  'ml-apis',
  'mysql',
  'workspace',
]
delete_images_cmd = 'gcloud container images list-tags gcr.io/$PROJECT_ID/$img --filter="TAGS!=latest" \
--format="get(digest)" | xargs -I {arg} gcloud container images delete \
"gcr.io/$PROJECT_ID/$img@{arg}" --quiet'


def create_dag():
    dag = DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval='@weekly',
        catchup=False,
        on_success_callback=cleanup_xcom,
        on_failure_callback=report_failure
    )

    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(task_id='finish')
        for img in images_list:
            task_id = f'delete_{img}_digests'
            delete_img_task = BashOperator(
                task_id=task_id,
                bash_command=delete_images_cmd,
                env={'img': img, 'PROJECT_ID': project_id}
            )
            start_task >> delete_img_task >> finish_task
    return dag


globals()[DAG_ID] = create_dag()
