from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.custom_operators import GetCheckpointOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.operators.custom_operators import SetCheckpointOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from unittest.mock import patch, MagicMock
from unittest import mock
import os
import bq_to_wrench
from libs.cleanup import cleanup_xcom
import pytest
from datetime import datetime
from libs import CloudStorage
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob
from libs import WrenchAuthWrapper, WrenchUploader
from requests.models import Response
import tempfile

DAG_ID = bq_to_wrench.DAG_ID
BUCKET_NAME = 'mock_bucket'
BLOB_NAME = 'staging.contacts-bluesun-13390c73-ef0a-4ddf-aeee-7d48720fa47.csv.gz'
storage_client = mock.create_autospec(CloudStorage)

seeds = [
    ('staging', [
        ('zleads', [
            {'lead_id': 1, 'first_name': 'Jon', 'last_name': 'Doe',  # noqa: E501
             'email': 'jon@test.com', 'phone': '1231111111', 'purchased': False,
             'icentris_client': 'monat', 'client_partition_id': 2,
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created': '1970-01-01'},
            {'lead_id': 2, 'first_name': 'Wington', 'last_name': 'Doe',  # noqa: E501
             'email': 'wington@test.com', 'phone': '1232222222', 'purchased': False,
             'icentris_client': 'monat', 'client_partition_id': 2,
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created': '1970-01-01'},
            {'lead_id': 3, 'first_name': 'Elise', 'last_name': 'Doe',  # noqa: E501
             'email': 'elise@test.com', 'phone': '1233333333', 'purchased': False,
             'icentris_client': 'monat', 'client_partition_id': 2,
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created': '1970-01-01'},
            {'lead_id': 4, 'first_name': 'Stu', 'last_name': 'Doe',  # noqa: E501
             'email': 'stu@test.com', 'phone': '1234444444', 'purchased': False,
             'icentris_client': 'monat', 'client_partition_id': 2,
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created': '1970-01-01'},
            {'lead_id': 5, 'first_name': 'Patrick', 'last_name': 'Doe',  # noqa: E501
             'email': 'patric@test.com', 'phone': '1235555555', 'purchased': False,
             'icentris_client': 'monat', 'client_partition_id': 2,
             'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
             'created': '1970-01-01'},
        ]),
        ('contacts', [
            {
                'client_partition_id': 4,
                'icentris_client': 'bluesun',
                'id': 1,
                'first_name': 'first_name',
                'last_name': 'last_name',
                'email': 'email1',
                'email2': 'email2',
                'email3': 'email3',
                'phone': 'phone',
                'phone2': 'phone2',
                'phone3': 'phone3',
                'twitter': 'twitter',
                'city': 'city',
                'state': 'state',
                'zip': 'zip',
                'country': 'country',
                'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
            }
        ])
    ])
]


def test_bigquery(seed, env, xcomm_mock, bigquery_helper):
    seed(seeds)
    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
    }
    query = bq_to_wrench.parse_query('staging.zleads', **xcomm_mock(checkpoint))  # noqa: E501
    resp = bigquery_helper.query(sql=query)
    assert len(resp) == 5


def test_build_query(xcomm_mock):
    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
    }

    with pytest.raises(SyntaxError):
        bq_to_wrench.build_query(table='unknown-table')

    query = str(bq_to_wrench.build_query(
        checkpoint=checkpoint,
        project='project',
        table='staging.zleads',
    ))

    assert "FROM `project.staging.zleads`" in query
    assert "ingestion_timestamp BETWEEN '1970-01-01 00:00:00.0 UTC' AND '2019-01-01 00:00:00.0 UTC'" in query  # noqa: E501

    query = str(bq_to_wrench.build_query(
        checkpoint=checkpoint,
        project='project',
        table='staging.contacts',
    ))

    assert query  # line above will throw an exception is contacts is missing.

    # query = str(bq_to_wrench.build_query(
    #     checkpoint=checkpoint,
    #     project='project',
    #     table='staging.contacts',
    #     tree_user_ids=[1, 2, 3, 4, 5],
    #     created_after_certain_date='1970-01-01'))

    # assert "AND tree_user_id in (1,2,3,4,5)" in query
    # assert "AND created >= '1970-01-01'" in query


def test_parse_query(xcomm_mock):
    expected_query = 'select * from table'

    with patch.object(bq_to_wrench, 'build_query', return_value=expected_query):  # noqa: E501
        checkpoint = {
            'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
            'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
        }
        query = bq_to_wrench.parse_query('some_table', **xcomm_mock(checkpoint))  # noqa: E501
        assert query == expected_query


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == bq_to_wrench.default_args
    if dag.catchup:
        assert False
    assert dag.on_success_callback == cleanup_xcom


def test_get_checkpoint_task(load_dag, env):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'get_checkpoint_{table}')
    assert isinstance(task, GetCheckpointOperator)
    # Not being used by GetCheckpointOperator
    # assert task._env == os.environ['ENV']
    assert task._target == table
    assert task._sources == [table]


def test_continue_if_data_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'continue_if_data_{table}')
    assert isinstance(task, BranchPythonOperator)
    assert task.python_callable.__name__ == bq_to_wrench.continue_if_data.__name__  # noqa: E501
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == table
    assert task.provide_context


def test_parse_query_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'parse_query_{table}')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == bq_to_wrench.parse_query.__name__
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == table
    assert task.provide_context


def test_dataflow_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'schedule_dataflow_{table}')
    assert isinstance(task, ScheduleDataflowJobOperator)
    assert task._template_name == 'offload_bq_to_cs'
    assert task._job_name == f'bq-to-wrench-{table}'
    assert task._pull_parameters == [{
        'param_name': 'query',
        'task_id': f'parse_query_{table}'
    }]
    assert task.provide_context


def test_monitor_dataflow_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'monitor_dataflow_{table}')
    assert isinstance(task, DataflowJobStateSensor)
    assert task._pusher_task_id == f'schedule_dataflow_{table}'


def test_offload_to_wrench_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'offload_to_wrench_{table}')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == bq_to_wrench.offload_to_wrench.__name__  # noqa: E501
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == os.environ['ENV']
    assert task.op_args[1] == table


def test_commit_checkpoint_task(load_dag):
    table = bq_to_wrench.tables[0]['table']
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'commit_checkpoint_{table}')
    assert isinstance(task, SetCheckpointOperator)


def test_build_status_insert():
    start_date = str(datetime.utcnow())
    query = bq_to_wrench.build_status_insert('bluesun', 'running', 'staging.zleads', 'foo.csv', start_date)
    assert str(query) == f"""INSERT INTO wrench.data_source_status (icentris_client, file, table, status, start_date) VALUES\n('bluesun','foo.csv','staging.zleads','running','{start_date}')""" # noqa:  E501


@patch('libs.BigQuery.query')
def test_insert_status_record(query):
    bq_to_wrench.build_status_insert = MagicMock(return_value='return_query')
    bq_to_wrench.insert_status_record('bluesun', 'running', 'table', {}, 'datetime_now')
    bq_to_wrench.build_status_insert.assert_called_once_with('bluesun', 'running', 'table', {}, 'datetime_now')
    query.assert_called_once_with('return_query')


@patch('shutil.rmtree')
def test_offload_to_wrench(mtree):
    expected_json = {
        'file': f'20210210122500-{BLOB_NAME}',
        'extra': 'data'
    }

    mock_bucket = mock.create_autospec(Bucket)
    response = mock.create_autospec(Response)
    response.status_code = 200
    response.json = MagicMock(return_value=expected_json)
    mock_blob = mock.create_autospec(Blob)
    mock_blob.name = BLOB_NAME
    mock_bucket.list_blobs.return_value = [mock_blob]
    bq_to_wrench.insert_status_record = MagicMock()
    dt_now = '2020-01-01 01:01:00.000000'
    bq_to_wrench.get_datetime_now = lambda: dt_now
    bq_to_wrench._move_failed_file = MagicMock()
    mock_api_key = {'host': 'http://foo.bar', 'api_key': 'asdf', 'expiry_time': '2070-01-01 00:00:00.000000001 +0000 UTC'}

    tmp = 'some/temp/dir'
    file_or_dir = '{}/{}'.format(tmp, BLOB_NAME)
    with patch.object(CloudStorage, 'factory', return_value=storage_client):
        with patch.object(storage_client, 'get_bucket', return_value=mock_bucket):
            with patch.object(WrenchUploader, 'upload', return_value=response):
                with patch.object(tempfile, 'mkdtemp', return_value=tmp):
                    with patch.object(os, 'mkdir', return_value=True) as mkdir:
                        with patch.object(WrenchAuthWrapper, 'get_api_key', return_value=mock_api_key):
                            bq_to_wrench.offload_to_wrench('local', 'staging.contacts')
                            mock_blob.download_to_filename.assert_called_once_with(file_or_dir)
                            storage_client.move_files.assert_called_once_with(
                                mock_bucket, mock_bucket, prefix=mock_blob.name)
                            bq_to_wrench.insert_status_record.assert_called_once_with(
                                'bluesun', 'running', 'staging.contacts', expected_json['file'], dt_now)
                            bq_to_wrench._move_failed_file.assert_not_called()
                            mkdir.assert_called_with(tmp)
