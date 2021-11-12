import os
import cdc_from_gcs_to_lake
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
from libs import GCLOUD as gcloud, CloudStorage
from airflow.models import Variable
from unittest.mock import patch
from unittest import mock
from google.cloud.storage.bucket import Bucket
from libs.cleanup import cleanup_xcom

airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
project_id = gcloud.project(os.environ['ENV'])
bucket = f'{project_id}-cdc-imports'
processed_bucket = f'{project_id}-cdc-imports-processed'
FILE_NAME = 'vibe-commission-bonuses-final-8b8e336ea2366596964f1ed2c67c3039bc5cfe57e823db8e647835f1fee26040-1587028509211'
FILE_SEED = """
14d1bbba50e40234839420171eb87431:0c81720eff1215c298621670f689ac76a3300ce0320c3a3c1c381d5f356f9fa405d14a9deabd0757207776d12a76bc076e2d0baaa6a79a0cb66b0ec2ee78005f05722934b501e1cb083bfedcc319e41dc0a207e899fcb9558f6c8826e3cee6beb67a0d1a878e4a5e86bb7f0579c28bcde88539add19e7aea69c495a413d2dc37892162d68b75e6003db81846bb96bfb946ef3d387a2b116b92a5b609b4c4e3c8570139f804daa04b105feeac06845efda0dce5360809de73d4c7831c9e84c4974313ebe7ea807093e2f214379f4c5e8c805fa4004cfc2f1c8cbf23ad68145a3a
"""
DAG_ID = cdc_from_gcs_to_lake.DAG_ID


def test_should_continue():
    storage = mock.create_autospec(CloudStorage)
    mock_bucket = mock.create_autospec(Bucket)
    prefix = 'a_file_that_starts_with'
    with patch.object(CloudStorage, 'factory', return_value=storage):
        with patch.object(storage, 'get_bucket', return_value=mock_bucket):
            with patch.object(storage, 'has_file', return_value=True):
                bucket = 'TO_BUCKET'
                table = 'table'
                assert cdc_from_gcs_to_lake.should_continue(
                    prefix=prefix,
                    bucket=bucket,
                    table=table
                ) == 'schedule_df_gcs_to_lake_table'

                storage.has_file.assert_called_once_with(bucket=mock_bucket, prefix=prefix)

        with patch.object(storage, 'has_file', return_value=False):
            assert cdc_from_gcs_to_lake.should_continue(
                prefix=prefix,
                bucket=bucket,
                table=table
            ) == 'finish'


def test_move_files(load_dag):
    cdc_imports_bucket = mock.create_autospec(Bucket)
    cdc_imports_bucket.name = 'cdc_imports_bucket'
    cdc_imports_processed_bucket = mock.create_autospec(Bucket)
    cdc_imports_processed_bucket.name = 'cdc_imports_processed_bucket'
    storage = mock.create_autospec(CloudStorage)
    with patch.object(CloudStorage, 'factory', return_value=storage):
        with patch.object(storage, 'get_bucket', side_effect=[cdc_imports_bucket, cdc_imports_processed_bucket]):
            with patch.object(storage, 'move_files', return_value=None) as p:
                prefix = 'starts-with-something'
                cdc_from_gcs_to_lake.move_files(prefix, 'cdc_imports_bucket', 'cdc_imports_processed_bucket')
                p.assert_called_once_with(cdc_imports_bucket, cdc_imports_processed_bucket, prefix=prefix)


def test_environment():
    """
    Test bucket naming is important because we need to setup in environment.py
    """
    assert cdc_from_gcs_to_lake.bucket == f'{project_id}-cdc-imports'
    assert cdc_from_gcs_to_lake.processed_bucket == f'{project_id}-cdc-imports-processed'


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == cdc_from_gcs_to_lake.default_args
    if dag.catchup:
        assert False
    assert dag.on_success_callback == cleanup_xcom


def test_continue_if_file_task(load_dag):
    table, prefix = list(cdc_from_gcs_to_lake.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'continue_if_file_{table}')
    assert isinstance(task, BranchPythonOperator)
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == table
    assert task.op_args[1] == cdc_from_gcs_to_lake.bucket
    assert task.op_args[2] == prefix
    assert task.python_callable.__name__ == cdc_from_gcs_to_lake.should_continue.__name__


def test_schedule_df_task(load_dag):
    prefix, table = list(cdc_from_gcs_to_lake.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'schedule_df_gcs_to_lake_{table}')
    assert isinstance(task, ScheduleDataflowJobOperator)
    assert task._template_name == 'load_cdc_from_gcs_to_lake'
    assert task._job_name == f'gcs-to-lake-{table}'
    project_id = cdc_from_gcs_to_lake.project_id
    assert project_id == gcloud.project(cdc_from_gcs_to_lake.env)
    assert task._job_parameters == {
        'files_startwith': prefix,
        'dest': f'{project_id}:lake.{table}'
    }
    assert task.provide_context


def test_monitor_dataflow_task(load_dag):
    prefix, table = list(cdc_from_gcs_to_lake.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'monitor_df_job_{table}')
    assert isinstance(task, DataflowJobStateSensor)
    assert task._pusher_task_id == f'schedule_df_gcs_to_lake_{table}'


def test_move_files_task(load_dag):
    prefix, table = list(cdc_from_gcs_to_lake.table_map.items())[0]
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task(f'move_processed_files_{prefix}')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == cdc_from_gcs_to_lake.move_files.__name__
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == prefix
    assert task.op_args[1] == cdc_from_gcs_to_lake.bucket
    assert task.op_args[2] == cdc_from_gcs_to_lake.processed_bucket
