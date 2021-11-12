from .config import Config
from .gcloud import GCLOUD
from google.cloud import secretmanager
from google.cloud.secretmanager import SecretManagerServiceClient, SecretVersion
from unittest.mock import patch, create_autospec
import os
from google.api_core.exceptions import NotFound, FailedPrecondition

env = os.environ['ENV']
project = GCLOUD.project(os.environ['ENV'])
secret = 'super-secret'
secret_version = 'latest'
payload = 'foo'


class response():
    class payload():
        data = 'foo'.encode('UTF-8')


def test_get_secret_version_path():
    assert Config.get_secret_version_path(env, secret) == f'projects/{project}/secrets/{secret}/versions/{secret_version}'
    assert Config.get_secret_version_path(env, secret, '1') == f'projects/{project}/secrets/{secret}/versions/1'
    secret_path_version = f'projects/{project}/secrets/another-secret'
    assert Config.get_secret_version_path(env, secret_path_version, 5) == f'projects/{project}/secrets/another-secret/versions/5'  # noqa: E501
    secret_path_version = f'projects/{project}/secrets/another-secret/versions/5'
    assert Config.get_secret_version_path(env, secret_path_version) == secret_path_version


def test_secret():
    with patch.object(Config, 'get_secret_version_path', return_value='foo'):
        with patch.object(Config, 'access_secret_version', return_value=response()):
            assert Config.secret(env, secret) == 'foo'
            Config.get_secret_version_path.assert_called_with(env, secret)
            Config.access_secret_version.assert_called_with(env, 'foo')


def test_create_secret():
    client = create_autospec(SecretManagerServiceClient)
    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        Config.create_secret(env, secret)
        kwargs = {'request': {
            'parent': f'projects/{project}', 'secret_id': 'super-secret',
            'secret': {'replication': {'automatic': {}}}}}
        client.create_secret.assert_called_with(**kwargs)


def test_add_secret_version():
    client = create_autospec(SecretManagerServiceClient)
    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        Config.add_secret_version(env, secret, payload)
        kwargs = {'request': {
            'parent': f'projects/{project}/secrets/{secret}',
            'payload': {'data': b'foo'}}}
        client.add_secret_version.assert_called_with(**kwargs)


def test_disable_secret_version():
    client = create_autospec(SecretManagerServiceClient)
    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        Config.disable_secret_version(env, secret, secret_version)
        kwargs = {'request': {
            'name': f'projects/{project}/secrets/{secret}/versions/{secret_version}'}}
        client.disable_secret_version.assert_called_with(**kwargs)


def test_list_secret_versions():
    client = create_autospec(SecretManagerServiceClient)
    kwargs = {
        'name': 'projects/939284484705/secrets/vibe-cdc/versions/1',
        'create_time': {
            'seconds': 1594700006,
            'nanos': 462944000
        },
        'state': 1,
        'replication_status': {
            'automatic': {}
        }
    }

    expected = [SecretVersion(**kwargs)]

    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        client.list_secret_versions.return_value = expected
        assert Config.list_secret_versions(env, secret) == expected
        kwargs = {'request': {
            'parent': f'projects/{project}/secrets/{secret}'}}
        client.list_secret_versions.assert_called_with(**kwargs)


def test_list_secret_keys():
    client = create_autospec(SecretManagerServiceClient)
    kwargs = {
        'name': 'projects/939284484705/secrets/vibe-cdc/versions/1',
        'create_time': {
            'seconds': 1594700006,
            'nanos': 462944000
        },
        'state': 1,
        'replication_status': {
            'automatic': {}
        }
    }

    expected = [SecretVersion(**kwargs)]

    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        with patch.object(Config, 'list_secret_versions', return_value=expected):
            assert len(list(Config.list_secret_keys(env, secret))) == 1


def test_access_secret_version():
    client = create_autospec(SecretManagerServiceClient)
    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        client.access_secret_version.return_value = 'foo'
        assert Config.access_secret_version(env, secret, secret_version) == 'foo'
        kwargs = {'request': {
            'name': f'projects/{project}/secrets/{secret}/versions/{secret_version}'}}
        client.access_secret_version.assert_called_with(**kwargs)


def test_access_secret_version_expired_key():
    client = create_autospec(SecretManagerServiceClient)
    with patch.object(secretmanager, 'SecretManagerServiceClient', return_value=client):
        client.access_secret_version.side_effect = [FailedPrecondition('Expired Key'), 'foo']
        assert Config.access_secret_version(env, secret, secret_version) is None


def test_add_secret():
    with patch.object(Config, 'add_secret_version'):
        Config.add_secret(env, secret, payload)
        Config.add_secret_version.assert_called_with(env, secret, payload)

    with patch.object(Config, 'add_secret_version', side_effect=[NotFound('foo'), 'ok']):
        with patch.object(Config, 'create_secret'):
            Config.add_secret(env, secret, payload)
            Config.create_secret.assert_called_with(env, secret)
            Config.add_secret_version.assert_called_with(env, secret, payload)
