from airflow import DAG
from datetime import datetime
from libs import report_failure
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
from libs.shared.bigquery import BigQuery

DAG_ID = 'wrench_tasks'
env = os.environ['ENV']

default_args = {
    'owner': 'airflow',
    'description': 'Generic Wrench tasks',  # noqa: E501
    'start_date': datetime(2020, 3, 27),
    'on_failure_callback': report_failure,
    'email_on_failure': False,
    'email_on_retry': False
}


def bq_update_lead_ids():
    '''
    https://stackoverflow.com/questions/48177241/google-bq-how-to-upsert-existing-data-in-tables
    '''
    client = BigQuery(env=env)
    client.query('''
        INSERT INTO `wrench.lead_entities_lk` (entity_id, lead_id)
            SELECT
                e.entity_id, z.lead_id
            FROM
                wrench.entities e
                INNER JOIN wrench.data_source_status dss ON (
                    e.icentris_client = dss.icentris_client
                    AND e.file = dss.file
                    AND dss.status = 'processed'
                    AND dss.end_date IS NULL)
                INNER JOIN staging.zleads z ON (e.icentris_client = z.icentris_client AND e.email = z.email)
                LEFT JOIN wrench.lead_entities_lk lk ON e.entity_id = lk.entity_id
            WHERE
                dss.table = 'staging.zleads'
                AND lk.entity_id IS NULL
    ''')


def bq_update_contact_ids():
    '''
    https://stackoverflow.com/questions/48177241/google-bq-how-to-upsert-existing-data-in-tables
    '''
    client = BigQuery(env=env)
    client.query('''
        INSERT INTO `wrench.contact_entities_lk` (entity_id, contact_id)
            SELECT
                e.entity_id,
                sc.id as contact_id
            FROM
                wrench.entities e
                INNER JOIN wrench.data_source_status dss ON (
                    e.icentris_client = dss.icentris_client
                    AND e.file = dss.file
                    AND dss.status = 'processed'
                    AND dss.end_date IS NULL)
                INNER JOIN staging.contacts sc ON (e.icentris_client = sc.icentris_client AND e.email = sc.email)
                LEFT JOIN wrench.contact_entities_lk lk ON e.entity_id = lk.entity_id
            WHERE
                dss.table = 'staging.contacts'
                AND lk.entity_id IS NULL
    ''')


def create_dag():
    dag = DAG(DAG_ID,
              default_args=default_args,
              schedule_interval='5 * * * *',
              catchup=False)
    with dag:
        start_task = DummyOperator(
            task_id='start'
        )

        bq_update_lead_ids_task = PythonOperator(
            task_id='bq_update_lead_ids_task',
            python_callable=bq_update_lead_ids
        )

        bq_update_lead_ids_task = PythonOperator(
            task_id='bq_update_contact_ids_task',
            python_callable=bq_update_contact_ids
        )

        start_task >> bq_update_lead_ids_task

    return dag


globals()[DAG_ID] = create_dag()
