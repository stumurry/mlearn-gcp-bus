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
def dataflow_docker_file():
    return parse_docker_file('/workspace/docker/dataflow/Dockerfile')


@pytest.fixture
def base_docker_file():
    return parse_docker_file('/workspace/docker/base/Dockerfile')


@pytest.fixture
def requirements_file():
    import importlib.util
    spec = importlib.util.spec_from_file_location("setup", "/workspace/dataflow/setup.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.REQUIRED_PACKAGES


def test_version_pinning(dataflow_docker_file, base_docker_file, requirements_file):
    for f in dataflow_docker_file:
        assert '==' in f
    for f in base_docker_file:
        assert '==' in f
    for f in requirements_file:
        assert '==' in f


def test_libraries_should_be_in_requirements_not_docker_files(dataflow_docker_file, base_docker_file, requirements_file):
    dataflow = remove_versions(dataflow_docker_file)
    base = remove_versions(base_docker_file)
    requirements = remove_versions(requirements_file)
    ignore = [
        'google-api-python-client',
        'google-cloud-storage',
        'cryptography',
        'neo4j'
    ]
    for r in requirements:
        assert r not in dataflow or r in ignore
        assert r not in base or r in ignore


def test_do_not_add_libraries_to_dockerfiles(dataflow_docker_file, base_docker_file, requirements_file):
    dataflow = remove_versions(dataflow_docker_file)
    base = remove_versions(base_docker_file)
    requirements = remove_versions(requirements_file)
    ignore = [
        'apache-beam[gcp]',
        'google-cloud-bigquery',
        'crcmod',
        'pytest',
        'pytest-cov',
        'faker',
        'bandit',
        'flake8',
        'google-cloud-kms',
        'boto3',
        'dockerfile'
    ]
    for r in dataflow:
        assert r in requirements or r in ignore
    for r in base:
        assert r in requirements or r in ignore
