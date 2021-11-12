import pytest
from airflow import DAG
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow import settings
from airflow.utils import timezone
from datetime import datetime, timedelta
from libs.cleanup import cleanup_xcom

now = datetime.utcnow()


@pytest.fixture(scope='module')
def dag():
    with DAG(dag_id='xcom-test-dag',
             default_args={'start_date': now},
             on_success_callback=cleanup_xcom) as dag:
        push_task = PythonOperator(
            task_id='push',
            python_callable=push_callable
        )
        pull_task = PythonOperator(
            task_id='pull',
            python_callable=pull_callable
        )
        push_task >> pull_task
        return dag


def push_callable():
    pass


def pull_callable(**kwargs):
    pass


def test_dagrun_success_callback(dag):
    dag.clear()
    execution_date = timezone.utcnow()
    print(f'test execution_date: {execution_date}')

    # Delete all xcoms for this DAG
    session = settings.Session()
    session.query(XCom).filter(XCom.dag_id == dag.dag_id).delete()
    zero_xcoms = XCom.get_many(
        dag_ids=[dag.dag_id],
        execution_date=execution_date,
        include_prior_dates=True)
    assert len(zero_xcoms) == 0

    push_ti = TaskInstance(task=dag.get_task('push'), execution_date=execution_date)

    # This xcom was set in a previous run
    XCom.set(
        key=None,
        value='foo',
        task_id=push_ti.task_id,
        dag_id=dag.dag_id,
        execution_date=execution_date - timedelta(minutes=5))

    # This xcom was set in the current run
    XCom.set(
        key=None,
        value='foo',
        task_id=push_ti.task_id,
        dag_id=dag.dag_id,
        execution_date=execution_date)

    previous_run_xcoms = XCom.get_many(
        dag_ids=[dag.dag_id],
        execution_date=execution_date - timedelta(minutes=5),
        include_prior_dates=False)
    assert len(previous_run_xcoms) == 1
    assert previous_run_xcoms[0].execution_date == execution_date - timedelta(minutes=5)

    current_run_xcoms = XCom.get_many(
        dag_ids=[dag.dag_id],
        execution_date=execution_date,
        include_prior_dates=False)
    assert len(current_run_xcoms) == 1
    assert current_run_xcoms[0].execution_date == execution_date

    task_states = {
        'push': State.SUCCESS,
        'pull': State.SUCCESS
    }

    dag_run = dag.create_dagrun(
        run_id=f'test-{datetime.utcnow()}',
        execution_date=execution_date,
        start_date=dag.start_date,
        state=State.RUNNING,
        external_trigger=False,
    )

    for task_id, task_state in task_states.items():
        ti = dag_run.get_task_instance(task_id)
        ti.set_state(task_state, session)

    dag_run.update_state()
    assert dag_run.state == State.SUCCESS

    # We still have the previous run xcom
    previous_run_xcoms = XCom.get_many(
        dag_ids=[dag.dag_id],
        execution_date=execution_date - timedelta(minutes=5),
        include_prior_dates=False)
    assert len(previous_run_xcoms) == 1
    assert previous_run_xcoms[0].execution_date == execution_date - timedelta(minutes=5)

    current_run_xcoms = XCom.get_many(
        dag_ids=[dag.dag_id],
        execution_date=execution_date,
        include_prior_dates=False)
    assert len(current_run_xcoms) == 0

    session.close()
