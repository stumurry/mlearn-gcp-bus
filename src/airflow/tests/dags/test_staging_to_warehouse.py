from airflow.operators.custom_operators import ScheduleDataflowJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.custom_sensors import DataflowJobStateSensor
import staging_to_warehouse
from unittest.mock import patch

DAG_ID = 'staging_to_warehouse'

table_map = {
    'warehouse.distributor_orders': []
}


def test_parse_query():
    assert staging_to_warehouse.table_map == table_map
    for table, sources in staging_to_warehouse.table_map.items():
        mock_kwargs = {
            'table': table
        }
        # exception thrown if file not found.
        staging_to_warehouse.parse_query(**mock_kwargs)

    with patch.object(staging_to_warehouse, 'parse_query', return_value='query string'):
        positional_test_argument = 'test_table'
        mock_args = [positional_test_argument]
        mock_kwargs = {'table': positional_test_argument}
        staging_to_warehouse.parse_query(*mock_args, **mock_kwargs)
        staging_to_warehouse.parse_query.assert_called_once_with(positional_test_argument, table=positional_test_argument)


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == staging_to_warehouse.default_args
    if dag.catchup:
        assert False


def test_parse_query_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task('parse_query_warehouse.distributor_orders')
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == staging_to_warehouse.parse_query.__name__
    for op_arg in task.op_args:
        assert isinstance(op_arg, str)
    assert task.op_args[0] == 'warehouse.distributor_orders'


def test_dataflow_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task('schedule_dataflow_warehouse.distributor_orders')
    assert isinstance(task, ScheduleDataflowJobOperator)
    assert task._template_name == 'load_staging_to_warehouse_distributor_orders'
    assert task._job_name == 'staging-to-warehouse-warehouse.distributor_orders'


def test_monitor_dataflow_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    task = dag.get_task('monitor_df_job_warehouse.distributor_orders')
    assert isinstance(task, DataflowJobStateSensor)
