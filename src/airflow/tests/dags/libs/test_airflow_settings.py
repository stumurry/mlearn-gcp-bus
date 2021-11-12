from airflow import settings
from airflow.models import Variable
from libs import get_airflow_vars
import json
import os

dags = os.listdir(f'{settings.DAGS_FOLDER}')
excluded_files = ['__init__.py']
dags = list(filter(lambda f: f.endswith('.py') and f not in excluded_files, dags))


def test_airflow_vars_are_loading():
    response = get_airflow_vars()
    for v in response.values():
        assert v
        assert v != {}


def get_key_name_path(full_path, value, paths):
    if isinstance(value, dict):
        for k, v in value.items():
            path = get_key_name_path(f'{full_path}.{k}', v, paths)
            if path is not None:
                paths.append(path)
    else:

        return full_path


def test_for_dag_variables_duplicates_in_both_files():
    airflow_vars = Variable.get('airflow_vars', deserialize_json=True)
    extended_variables = {}
    with open(f'{settings.DAGS_FOLDER}/extended_variables.json', 'r') as f:
        file_json_content = f.read()
        extended_variables = json.loads(file_json_content)
    airflow_vars_keys = []
    extended_variables_keys = []
    for k, v in airflow_vars.items():
        if isinstance(v, dict):
            for kv, vv in v.items():
                v_paths = []
                get_key_name_path(f'{k}.{kv}', vv, v_paths)
                airflow_vars_keys.extend(v_paths)
        elif v is not None:
            airflow_vars_keys.append(k)
    for k, v in extended_variables.items():
        if isinstance(v, dict):
            for kv, vv in v.items():
                e_paths = []
                get_key_name_path(kv, vv, e_paths)
                extended_variables_keys.extend(e_paths)
        elif v is not None:
            extended_variables_keys.append(k)

    for p in airflow_vars_keys:
        assert p not in extended_variables_keys
