import os
from libs.shared.gcloud import GCLOUD as gcloud
from libs.shared.test import BigQueryFixture
from libs.shared.test import CloudStorageFixture
import pytest
from libs.shared.config import Config
from unittest.mock import patch


@pytest.fixture()
def server():
    with patch.object(Config, 'secret', returns='foo'):
        import server as _server
        _server.app.testing = True
        return _server


@pytest.fixture()
def test_client(server):
    with server.app.test_client() as c:
        return c


@pytest.fixture(scope='module')
def bigquery(env):
    helper = BigQueryFixture.instance(env['env'])
    return helper


@pytest.fixture(scope='session')
def cloudstorage(env):
    return CloudStorageFixture.instance(env['env'])


# # flake8: noqa
# @pytest.fixture(scope='session')
# def secrets(env):
#     return SecretsFixture(env['env'])


@pytest.fixture(scope='session')
def env():
    return {
        'env': os.environ['ENV'],
        'project': gcloud.project(os.environ['ENV']),
        'user': gcloud.config()['username'],
        'client': 'bluesun'
    }
