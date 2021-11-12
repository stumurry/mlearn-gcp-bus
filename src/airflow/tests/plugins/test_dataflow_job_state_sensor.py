from airflow import DAG as dag
from airflow.models import Variable
from airflow.utils import timezone
from plugins.sensors import DataflowJobStateSensor
from datetime import datetime
from unittest.mock import patch
import pytest


airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
task_id = 'monitor_df_job_'
pusher_task_id = 'schedule_df_gcs_to_lake_'
dag_id = 'TEST_DAG'
DEFAULT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
d = dag(dag_id, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})


def test_pusher_task_id():
    sensor = DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id, dag=d)
    assert sensor._pusher_task_id == pusher_task_id


def test_poke_interval_and_timeout():
    poke_interval = airflow_vars['sensors']['dataflow_job_state_sensor']['poke_interval']
    poke_timeout = airflow_vars['sensors']['dataflow_job_state_sensor']['poke_timeout']
    sensor = DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id, dag=d)
    assert sensor.poke_interval == poke_interval
    assert sensor.timeout == poke_timeout


def test_dag_detached():
    with pytest.raises(Exception):
        DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id)


def test_dag_id():
    sensor = DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id, dag=d)
    assert sensor.dag_id == dag_id


def test_default_poke_interval():

    airflow_vars = {
        'sensors': {
            'dataflow_job_state_sensor': {
                'poke_interval': 54321,
                'poke_timeout': 12345
            }
        }
    }

    with patch.object(Variable, 'get', return_value=airflow_vars):
        poke_interval = airflow_vars['sensors']['dataflow_job_state_sensor']['poke_interval']
        poke_timeout = airflow_vars['sensors']['dataflow_job_state_sensor']['poke_timeout']
        sensor = DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id, dag=d)
        assert sensor.poke_interval == poke_interval
        assert sensor.timeout == poke_timeout


def test_table_values_should_ovveride_default():

    airflow_vars = {
        'sensors': {
            'dataflow_job_state_sensor': {
                'poke_interval': 54321,
                'poke_timeout': 12345
            }
        },
        'dags': {
            'TEST_DAG': {
                'poke_interval': 9999,
                'poke_timeout': 8888
            }
        }
    }

    with patch.object(Variable, 'get', return_value=airflow_vars):
        poke_interval = airflow_vars['dags'][dag_id].get('poke_interval')
        poke_timeout = airflow_vars['dags'][dag_id].get('poke_timeout')
        sensor = DataflowJobStateSensor(task_id=task_id, pusher_task_id=pusher_task_id, dag=d)
        assert sensor.poke_interval == poke_interval
        assert sensor.timeout == poke_timeout
