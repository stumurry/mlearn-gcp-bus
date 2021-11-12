import dockerfile
import pytest
import re


def parse_docker_file(file):
    commands = dockerfile.parse_file(file)
    pip_map = map(lambda s: s.original.split(' '),
                  filter(lambda s: s.cmd == 'run' and 'pip' in s.original, commands))
    exclusions = ['RUN', 'pip', 'install', '', '.']

    libs = []
    for m in pip_map:
        for mm in m:
            if mm not in exclusions and not mm.startswith('--') and not mm.startswith('-'):
                libs.append(mm)

    return libs


def remove_versions(arr):
    def find_all(s):
        r = re.findall(r"[\w\-\[\]]+", s)
        return r[0] if r else ''
    return list(map(lambda s: find_all(s).lower(), arr))


@pytest.fixture
def airflow_docker_file():
    return parse_docker_file('/workspace/docker/airflow/Dockerfile')


@pytest.fixture
def base_docker_file():
    return parse_docker_file('/workspace/docker/base/Dockerfile')


@pytest.fixture
def requirements_file():
    lines = []
    with open('/workspace/airflow/dags/requirements.txt', 'r') as f:
        for i in f.readlines():
            lines.append(i.rstrip())
    return lines


def test_version_pinning(airflow_docker_file, base_docker_file, requirements_file):
    for f in airflow_docker_file:
        assert '==' in f
    for f in base_docker_file:
        assert '==' in f
    for f in requirements_file:
        assert '==' in f


def test_anything_added_to_req_shouldnt_be_in_docker_files(airflow_docker_file, base_docker_file, requirements_file):
    airflow = remove_versions(airflow_docker_file)
    base = remove_versions(base_docker_file)
    requirements = remove_versions(requirements_file)
    ignore = []
    for r in requirements:
        assert r not in airflow or r in ignore
        assert r not in base or r in ignore


def test_anything_added_to_dockerfiles_must_be_in_requirements(airflow_docker_file, base_docker_file, requirements_file):
    airflow = remove_versions(airflow_docker_file)
    base = remove_versions(base_docker_file)
    requirements = remove_versions(requirements_file)
    ignore = [
        'werkzeug',
        'sqlalchemy',
        'apache-airflow[gcp]',
        'watchgod',
        'crcmod',
        'pytest',
        'pytest-cov',
        'faker',
        'bandit',
        'flake8',
        'dockerfile'
    ]
    for r in airflow:
        assert r in requirements or r in ignore
    for r in base:
        assert r in requirements or r in ignore
