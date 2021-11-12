from airflow.operators.custom_operators import ScheduleDataflowJobOperator
import wrench_to_bq
from libs.cleanup import cleanup_xcom
from datetime import datetime
from datetime import timedelta
import pytest
from requests import Response
import requests
from unittest.mock import patch
from libs import WrenchAuthWrapper
from libs import BigQuery

DAG_ID = wrench_to_bq.DAG_ID

seeds = [
    ('wrench', [
        ('data_source_status', [
            {
                'icentris_client': 'bluesun',
                'file': 'old-file.csv',
                'status': 'running',
                'table': 'staging.zleads',
                'start_date': '1970-01-01 00:00:00.0'
            },
            {
                'icentris_client': 'bluesun',
                'file': 'new-file.csv',
                'status': 'running',
                'table': 'staging.zleads',
                'start_date': str(datetime.utcnow())
            }
        ])
    ])]


@pytest.fixture
def future_api_key():
    fd = datetime.utcnow() + timedelta(days=1)
    return {
        'api_key': 'foobar',
        'expiry_time': f"{fd.strftime('%Y-%m-%d %H:%M:%S')}.000000001 +0000 UTC",
        'host': 'http://foo.bar'
    }


@pytest.fixture
def mock_entities():
    return [
        {'entity_id': '11111', 'email': 'test1@test.com', 'phone': '1111111111', 'twitter': '@test1'},
        {'entity_id': '22222', 'email': 'test2@test.com', 'phone': '2222222222', 'twitter': '@test2'}
    ]


@pytest.fixture
def mock_200_response():
    response = Response()
    response.status_code = 200
    return response


@pytest.fixture
def mock_status_response():
    def _mock(status):
        return {'start': '', 'status': status, 'stop': ''}

    return _mock


@pytest.fixture
def mock_file_record():
    return {
        'icentris_client': 'bluesun',
        'table': 'staging.zleads',
        'file': '20210210122500-bluesun-staging.zleads.GUID.csv',
        'status': 'SUCCEEDED'
    }


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == wrench_to_bq.default_args
    if dag.catchup:
        assert False
    assert dag.on_success_callback == cleanup_xcom


def test_get_running_files(seed, load_dag):
    seed(seeds)
    rs = wrench_to_bq.get_running_files()
    assert len(rs) == 1
    assert rs[0]['file'] == 'new-file.csv'


@pytest.mark.skip(reason='WIP')
def test_df_insights_task(load_dag):
    table = wrench_to_bq.tables[0]['table']
    insight_id = wrench_to_bq.tables[0]['insight_ids'][0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    d_name = insight_id.replace('-', '_')
    dest = f'wrench.{d_name}'
    pusher_id = f'{table}_schedule_load_wrench_to_bq_{d_name}'
    pull_id = f'{table}_continue_if_unprocessed_{d_name}'
    task = dag.get_task(pusher_id)
    assert isinstance(task, ScheduleDataflowJobOperator)
    assert task._template_name == 'load_wrench_to_bq'
    assert task._job_name == f'{table}_load_wrench_to_bq_{d_name}'
    assert task._job_parameters == {
        'src_table': table,
        'dest': dest,
        'insight_id': insight_id
    }
    assert task._pull_parameters == [
        {'key': f'{pull_id}_statuses', 'param_name': 'file_statuses'}
    ]


def test_fetch_entities(future_api_key, mock_entities, mock_200_response, mock_file_record):
    with patch.object(WrenchAuthWrapper, 'get_api_key', return_value=future_api_key):
        with patch.object(requests, 'post', return_value=mock_200_response):
            with patch.object(Response, 'json', return_value=mock_entities):
                entities = wrench_to_bq.fetch_entities(mock_file_record)
                assert entities == mock_entities


def test_should_continue():
    continue_path = 'my_task_id'
    dest_table = 'foo'
    with patch.object(BigQuery, 'query', return_value=[1]):
        assert wrench_to_bq.should_continue(continue_path, dest_table) == continue_path
    with patch.object(BigQuery, 'query', return_value=[]):
        assert wrench_to_bq.should_continue(continue_path, dest_table) == 'finish'


def test_update_status(seed, mock_file_record):
    seed(seeds)
    wrench_to_bq.update_status(seeds[0][1][0][1][0], 'succeeded')
    result = wrench_to_bq.get_running_files()
    assert len(result) == 1


def test_fetch_job_status_succeeded(future_api_key, mock_200_response, mock_status_response, mock_file_record):
    msr = mock_status_response('SUCCEEDED')
    with patch.object(WrenchAuthWrapper, 'get_api_key', return_value=future_api_key):
        with patch.object(requests, 'post', return_value=mock_200_response):
            with patch.object(Response, 'json', return_value=msr):
                result = wrench_to_bq.fetch_job_status(mock_file_record)
                assert result['status'] == 'SUCCEEDED'


def test_fetch_job_status_throws(future_api_key):
    mock_500_response = Response()
    mock_500_response.status_code = 500
    with patch.object(WrenchAuthWrapper, 'get_api_key', return_value=future_api_key):
        with patch.object(requests, 'post', return_value=mock_500_response):
            with pytest.raises(Exception):
                wrench_to_bq.fetch_job_status('foo.csv')


def test_import_entities_status_failed(bigquery_helper, seed):
    seed(seeds)
    mock_files = [{'icentris_client': 'bluesun', 'file': 'old-file.csv', 'status': 'RUNNING', 'table': 'staging.zleads'}]
    with patch.object(wrench_to_bq, 'get_running_files', return_value=mock_files):
        with patch.object(wrench_to_bq, 'fetch_job_status', return_value={'status': 'FAILED'}):
            with patch.object(wrench_to_bq, 'update_status') as mock_update_status:
                wrench_to_bq.import_entities()
    mock_update_status.assert_called_once_with(mock_files[0], 'FAILED')


def test_import_entities(bigquery_helper, seed, mock_entities):
    seed(seeds)
    mock_files = [{'icentris_client': 'bluesun', 'file': 'old-file.csv', 'status': 'RUNNING', 'table': 'staging.zleads'}]
    with patch.object(wrench_to_bq, 'get_running_files', return_value=mock_files):
        with patch.object(wrench_to_bq, 'fetch_job_status', return_value={'status': 'SUCCEEDED'}):
            with patch.object(wrench_to_bq, 'update_status') as mock_update_status:
                with patch.object(wrench_to_bq, 'fetch_entities', return_value=mock_entities):
                    with patch.object(wrench_to_bq, 'insert_entities') as mock_insert_entities:
                        wrench_to_bq.import_entities()

    mock_update_status.assert_called_once_with(mock_files[0], 'SUCCEEDED')
    mock_insert_entities.assert_called_once_with(mock_files[0], mock_entities)


def test_import_entities_short_circuits(bigquery_helper, seed):
    # Test that update_status is not called if an error occurs in fetch_entities
    seed(seeds)
    mock_files = [{'icentris_client': 'bluesun', 'file': 'old-file.csv', 'status': 'RUNNING', 'table': 'staging.zleads'}]
    with patch.object(wrench_to_bq, 'get_running_files', return_value=mock_files):
        with patch.object(wrench_to_bq, 'fetch_job_status', return_value={'status': 'SUCCEEDED'}):
            with patch.object(wrench_to_bq, 'fetch_entities', side_effect=Exception('error')):
                with patch.object(wrench_to_bq, 'update_status') as mock_update_status:
                    with pytest.raises(Exception):
                        wrench_to_bq.import_entities()
    mock_update_status.assert_not_called()
