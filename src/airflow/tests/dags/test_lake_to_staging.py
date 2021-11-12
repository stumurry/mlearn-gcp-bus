from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.custom_operators import GetCheckpointOperator, SetCheckpointOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
import lake_to_staging
from unittest.mock import patch
from libs import GCLOUD as gcloud
from libs.cleanup import cleanup_xcom

DAG_ID = lake_to_staging.DAG_ID

table_map = {
    'staging.zleads': ['lake.zleads'],
    'staging.contacts': ['lake.contacts']
}


def test_parse_query(xcomm_mock):
    assert lake_to_staging.table_map == table_map

    for table, sources in lake_to_staging.table_map.items():
        checkpoint = {
            'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
            'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC'
        }
        # exception thrown if file not found.
        lake_to_staging.parse_query(table, **xcomm_mock(checkpoint))

    with patch.object(lake_to_staging, 'parse_query', return_value='query string'):
        positional_test_argument = 'test_table'
        mock_args = [positional_test_argument]
        mock_kwargs = {'table': positional_test_argument}
        lake_to_staging.parse_query(*mock_args, **mock_kwargs)
        lake_to_staging.parse_query.assert_called_once_with(positional_test_argument, table=positional_test_argument)


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == lake_to_staging.default_args
    if dag.catchup:
        assert False
    assert dag.on_success_callback == cleanup_xcom


def test_get_checkpoint_task(load_dag, env):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'get_checkpoint_{table}')
    assert isinstance(task, GetCheckpointOperator)
    # Not being used by GetCheckpointOperator
    # assert task._env == os.environ['ENV']
    assert task._target == table
    assert task._sources == sources


def test_continue_if_data_task(load_dag):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'continue_if_data_{table}')
    assert isinstance(task, BranchPythonOperator)
    assert task.python_callable.__name__ == lake_to_staging.should_continue.__name__
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == table
    assert task.provide_context


def test_parse_query_task(load_dag):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'parse_query_{table}')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == lake_to_staging.parse_query.__name__
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == table
    assert task.provide_context


def test_dataflow_task(load_dag):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    parsed_table_name = gcloud.parse_table_name(table)
    task = dag.get_task(f'schedule_dataflow_{table}')
    assert isinstance(task, ScheduleDataflowJobOperator)
    assert task._template_name == f'load_lake_to_staging_{parsed_table_name}'
    assert task._job_name == f'lake-to-staging-{table}'
    assert task.provide_context


def test_monitor_dataflow_task(load_dag):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'monitor_df_job_{table}')
    assert isinstance(task, DataflowJobStateSensor)
    assert task._pusher_task_id == f'schedule_dataflow_{table}'


def test_set_checkpoint_task(load_dag):
    table, sources = list(lake_to_staging.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'set_checkpoint_{table}')
    assert isinstance(task, SetCheckpointOperator)
    assert task._table == table
