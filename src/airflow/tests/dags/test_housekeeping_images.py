import os
from libs import GCLOUD as gcloud
import housekeeping_images

DAG_ID = 'housekeeping_images'
env = os.environ['ENV']
project_id = gcloud.project(env)


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == housekeeping_images.default_args
    assert dag.on_success_callback == housekeeping_images.cleanup_xcom
    assert dag.on_failure_callback == housekeeping_images.report_failure
    assert dag.schedule_interval == '@weekly'


def test_delete_img_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    img_list = housekeeping_images.images_list
    cmd = housekeeping_images.delete_images_cmd
    for img in img_list:
        task_id = f'delete_{img}_digests'
        task = dag.get_task(task_id)
        assert task.bash_command == cmd
