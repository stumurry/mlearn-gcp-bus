"""
$dag_filename$: lake_to_staging.py
"""

import logging
import os
from libs import GCLOUD as gcloud, parse_template
from airflow import DAG, settings
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from airflow.operators.custom_operators import GetCheckpointOperator, SetCheckpointOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from libs import report_failure
from libs.cleanup import cleanup_xcom

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

"""
Composer Webserver only handles dynamic tasks creation with variables
existing either in the DAG file or in the Airflow env which gets imported from
variables.json. In order to maximize memory resources,
place variables to generate dynamic tasks in the DAG file. Wbrito 7/07/2020
"""
table_map = {
    # 'staging.contacts': [
    #                     'lake.pyr_contacts', 'lake.pyr_contact_emails',
    #                     'lake.pyr_contact_phone_numbers',
    #                     'lake.pyr_contacts_contact_categories'
    #                     ],
    # 'staging.orders': ['lake.tree_orders', 'lake.tree_order_items'],
    # 'staging.users': ['lake.tree_users', 'lake.users'],
    # 'pii.users': ['lake.tree_users']
    'staging.zleads': ['lake.zleads'],
    'staging.contacts': ['lake.contacts']
}


def _marker(st):
    logging.info('********************************{}*****************************************'.format(st))


def should_continue(table, **kwargs):
    checkpoint = kwargs['ti'].xcom_pull(key=table)
    if isinstance(checkpoint, dict) and checkpoint['has_data'] is True:
        return f'parse_query_{table}'
    else:
        return 'finish'


def parse_query(table, **kwargs):
    checkpoint = kwargs['ti'].xcom_pull(key=table)
    query = parse_template(f'{settings.DAGS_FOLDER}/templates/sql/lake_to_{table}.sql', **checkpoint)
    return query


DAG_ID = 'lake_to_staging'


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              # Be sure to stagger the dags so they don't run all at once,
              # possibly causing max memory usage and pod failure. - Stu M.
              schedule_interval='15 * * * *',
              catchup=False,
              on_success_callback=cleanup_xcom)
    with dag:
        start_task = DummyOperator(task_id='start')
        finish_task = DummyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        for table, sources in table_map.items():
            pusher_task_id = f'schedule_dataflow_{table}'

            get_checkpoint_task = GetCheckpointOperator(
                task_id=f'get_checkpoint_{table}',
                env=env,
                target=table,
                sources=sources
            )

            continue_if_data_task = BranchPythonOperator(
                task_id=f'continue_if_data_{table}',
                python_callable=should_continue,
                op_args=[table],
                provide_context=True
            )

            parse_query_task = PythonOperator(
                task_id=f'parse_query_{table}',
                python_callable=parse_query,
                op_args=[table],
                provide_context=True
            )

            t = table.replace('.', '_')
            dataflow_task = ScheduleDataflowJobOperator(
                task_id=pusher_task_id,
                project=gcloud.project(env),
                template_name=f'load_lake_to_{t}',
                job_name=f'lake-to-staging-{table}',
                job_parameters={'env': env},
                pull_parameters=[{
                    'param_name': 'query',
                    'task_id': f'parse_query_{table}'
                }],
                provide_context=True
            )

            monitor_dataflow_task = DataflowJobStateSensor(
                task_id=f'monitor_df_job_{table}',
                dag=dag,
                pusher_task_id=pusher_task_id
            )

            set_checkpoint_task = SetCheckpointOperator(
                task_id=f'set_checkpoint_{table}',
                env=env,
                table=table
            )

            start_task.set_downstream(get_checkpoint_task)
            get_checkpoint_task.set_downstream(continue_if_data_task)
            continue_if_data_task.set_downstream(parse_query_task)
            parse_query_task.set_downstream(dataflow_task)
            dataflow_task.set_downstream(monitor_dataflow_task)
            monitor_dataflow_task.set_downstream(set_checkpoint_task)
            set_checkpoint_task.set_downstream(finish_task)

        start_task >> finish_task
    return dag


globals()[DAG_ID] = create_dag()
