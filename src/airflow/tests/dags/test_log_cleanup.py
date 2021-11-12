import os
from unittest.mock import patch
from unittest import mock
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from libs import CloudStorage
import log_cleanup
from datetime import datetime
from airflow import settings
from google.cloud.storage.blob import Blob

airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
env = os.environ['ENV']


def test_find_log_blobs():
    bucket_name = 'TEST_BUCKET'
    text_to_search = [
        'airflow.cfg',
        'b45d3409-57b2-4300-a245-358fa2bad93f',
        'ba5d104a-9705-4f60-9afd-45edfc3bb66a',
        'dags/',
        'dags/airflow_monitoring.py',
        'logs/cdc_from_gcs_to_lake/move_processed_files_vibe-contact-phone-numbers-final/2020-06-10T23:00:00+00:00/',
        'logs/cdc_from_gcs_to_lake/move_processed_files_vibe-contact-phone-numbers-final/2020-04-10T23:00:00+00:00/1.log',
        'logs/cdc_from_gcs_to_lake/move_processed_files_vibe-contact-phone-numbers-final/2020-03-10T23:00:00+00:00/1.log',
        'logs/cdc_from_gcs_to_lake/move_processed_files_vibe-contact-phone-numbers-final/2020-06-10T23:00:00+00:00/1.log',
        'dags/',
        'dags/airflow_monitoring.py',
    ]
    blobs = map(lambda name: Blob(name=name, bucket=bucket_name), text_to_search)
    assert len(list(log_cleanup.find_log_blobs(blobs))) == 3


def test_filter_log_blobs_by_days_past():
    bucket_name = 'test_bucket'
    days_past = 30
    date_now = datetime.strptime('2020-06-01', '%Y-%m-%d')
    dates = ['2020-06-01', '2020-05-01']

    def get_tuple(date):
        return (
            datetime.strptime(date, '%Y-%m-%d').date(),
            Blob(name='TEST_BLOB', bucket=bucket_name)
        )
    with patch.object(log_cleanup, 'find_log_blobs', return_value=map(get_tuple, dates)):
        assert len(list(log_cleanup.filter_log_blobs_by_days_past([], date_now, days_past))) == 1


def test_clear_log_files_by_bucket():
    bucket_name = 'test_bucket'
    date_now = '2020-06-01'
    days_past = '30'

    def get_mock_blob(name):
        mock_blob = mock.create_autospec(Blob)
        mock_blob.name = name
        return mock_blob
    mock_blobs = list(map(lambda name: get_mock_blob(name), ['2020-06-01/1.log', '2020-04-01/2.log']))
    storage = mock.create_autospec(CloudStorage)
    with patch.object(CloudStorage, 'factory', return_value=storage):
        with patch.object(storage, 'list_blobs', return_value=mock_blobs):
            log_cleanup.clear_log_files_by_bucket(bucket_name, date_now, days_past)
            mock_blobs[1].delete.assert_called_once()


def test_clear_log_files_by_bucket_task(load_dag):
    dag_bag = load_dag('log_cleanup')
    dag = dag_bag.get_dag('log_cleanup')
    task = dag.get_task('clear_log_files_task')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == log_cleanup.clear_log_files_by_bucket.__name__

    for op_arg in task.op_args:
        assert isinstance(op_arg, str)

    assert task.op_args[0] == settings.DAGS_FOLDER
    assert task.op_args[1] == log_cleanup.date_now
    assert task.op_args[2] == log_cleanup.days_past
