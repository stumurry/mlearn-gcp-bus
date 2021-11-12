from .wrench import WrenchAuthWrapper
from .wrench import WrenchUploader
from unittest.mock import patch
from .config import Config
import pytest
from datetime import datetime, timedelta
import requests
from requests import Response
from requests.exceptions import HTTPError
import gzip


@pytest.fixture
def mocked_config():
    return {
        'wrench': {
            'api': {
                'bluesun': {
                    'host': 'http://foo.bar',
                    'username': 'wrench.user',
                    'password': 'wrench.pass'
                }
            }
        }
    }


@pytest.fixture
def new_auth_instance():
    i = WrenchAuthWrapper.instance('local')
    i.clear()
    return i


@pytest.fixture
def future_api_key():
    fd = datetime.utcnow() + timedelta(days=1)
    return {
        'api_key': 'foobar',
        'expiry_time': f"{fd.strftime('%Y-%m-%d %H:%M:%S')}.000000001 +0000 UTC",
        'host': 'http://foo.bar'
    }


@pytest.fixture
def expired_api_key():
    return {
        'api_key': 'foobar',
        'expiry_time': '1970-01-01 00:00:00.000000001 +0000 UTC',
        'host': 'http://foo.bar'
    }


def test_import():
    assert WrenchAuthWrapper is not None


def test_has_instance_method():
    assert hasattr(WrenchAuthWrapper, 'instance')


def test_has_api_key_attr(new_auth_instance):
    assert hasattr(new_auth_instance, 'api_keys')


def test_has_me_attr():
    assert hasattr(WrenchAuthWrapper, 'me')


def test_returns_instance():
    assert isinstance(WrenchAuthWrapper.instance('local'), WrenchAuthWrapper)


def test_has_get_api_key_method():
    assert hasattr(WrenchAuthWrapper, 'get_api_key')


def test_has_get_connection_info_method():
    assert hasattr(WrenchAuthWrapper, 'get_connection_info')


def test_has_is_expired_method():
    assert hasattr(WrenchAuthWrapper, 'is_expired')


def test_has_clear_method():
    assert hasattr(WrenchAuthWrapper, 'clear')


def test_clear_clears_api_key(new_auth_instance):
    new_auth_instance.api_keys['bluesun'] = {}
    assert new_auth_instance.api_keys['bluesun'] == {}
    new_auth_instance.clear()
    assert new_auth_instance.api_keys == {}


def test_get_connection_info_params(new_auth_instance, mocked_config):
    with patch.object(Config, 'get_config', return_value=mocked_config):
        info = new_auth_instance.get_connection_info('bluesun')
        assert mocked_config['wrench']['api']['bluesun'] == info


def test_raises_if_wrench_config_missing(new_auth_instance):
    with patch.object(Config, 'get_config', return_value={}):
        with pytest.raises(KeyError) as e:
            new_auth_instance.get_connection_info('bluesun')
        assert 'wrench key not found' in str(e.value)


def test_raises_if_api_config_missing(new_auth_instance):
    with patch.object(Config, 'get_config', return_value={'wrench': {}}):
        with pytest.raises(KeyError) as e:
            new_auth_instance.get_connection_info('bluesun')
        assert 'api key not found' in str(e.value)


def test_is_expired_true(new_auth_instance, expired_api_key):
    # No api_key should return as expired
    assert new_auth_instance.is_expired('bluesun') is True

    new_auth_instance.api_keys['bluesun'] = expired_api_key
    assert new_auth_instance.api_keys['bluesun'] == expired_api_key
    assert new_auth_instance.is_expired('bluesun') is True


def test_is_expired_false(new_auth_instance, future_api_key):
    new_auth_instance.api_keys['bluesun'] = future_api_key
    assert new_auth_instance.api_keys['bluesun'] == future_api_key
    assert new_auth_instance.is_expired('bluesun') is False


def test_is_expired_handles_faulty_dates(new_auth_instance, future_api_key):
    future_api_key['expiry_time'] = '1970-01-01'
    new_auth_instance.api_keys['bluesun'] = future_api_key
    assert new_auth_instance.api_keys['bluesun'] == future_api_key
    assert new_auth_instance.is_expired('bluesun') is True


def test_get_api_key_caches(new_auth_instance, future_api_key, mocked_config):
    mock_response = Response()
    mock_response.status_code = 200
    rv = mocked_config['wrench']['api']['bluesun']

    with patch.object(WrenchAuthWrapper, 'get_connection_info', return_value=rv) as get_con_mock:
        with patch.object(requests, 'post', return_value=mock_response):
            with patch.object(Response, 'json', return_value=future_api_key):
                # Starts with no value
                assert new_auth_instance.api_keys == {}

                # Returns and caches expected value
                result = new_auth_instance.get_api_key('bluesun')
                assert result == future_api_key
                assert new_auth_instance.api_keys['bluesun'] == future_api_key

                # Call it again. We want to ensure that get_connection_info
                # is not called a second time.
                new_auth_instance.get_api_key('bluesun')

    get_con_mock.assert_called_once_with('bluesun')


def test_get_api_key_refreshes(new_auth_instance, future_api_key, expired_api_key, mocked_config):
    mock_response = Response()
    mock_response.status_code = 200
    rv = mocked_config['wrench']['api']['bluesun']

    with patch.object(WrenchAuthWrapper, 'get_connection_info', return_value=rv) as get_con_mock:
        with patch.object(requests, 'post', return_value=mock_response):
            with patch.object(Response, 'json', return_value=future_api_key):
                # Starts with no value
                assert new_auth_instance.api_keys == {}

                # Set to expired key
                new_auth_instance.api_keys['bluesun'] = expired_api_key
                assert new_auth_instance.api_keys['bluesun'] == expired_api_key

                # Returns and caches expected value
                result = new_auth_instance.get_api_key('bluesun')
                assert result == future_api_key
                assert new_auth_instance.api_keys['bluesun'] == future_api_key

    assert get_con_mock.called_once_with('bluesun')


def test_get_api_key_fails_on_non_200(new_auth_instance, future_api_key, expired_api_key, mocked_config):
    mock_response = Response()
    mock_response.status_code = 500
    rv = mocked_config['wrench']['api']['bluesun']

    with patch.object(WrenchAuthWrapper, 'get_connection_info', return_value=rv):
        with patch.object(requests, 'post', return_value=mock_response):
            with pytest.raises(HTTPError):
                new_auth_instance.get_api_key('bluesun')


def test_has_truncate_nanoseconds_method():
    assert hasattr(WrenchAuthWrapper, 'truncate_nanoseconds')


def test_truncate_nanoseconds(new_auth_instance):
    ds = '2020-12-19 22:14:15.886187652 +0000 UTC'
    new_ds = new_auth_instance.truncate_nanoseconds(ds)
    assert new_ds == '2020-12-19 22:14:15.886187 +0000 UTC'


def test_truncate_nanoseconds_returns_original_on_error(new_auth_instance):
    ds = '2020-12-19'
    new_ds = new_auth_instance.truncate_nanoseconds(ds)
    assert new_ds == ds


def test_enriches_api_key_with_host(new_auth_instance, mocked_config):
    mock_response = Response()
    mock_response.status_code = 200
    rv = mocked_config['wrench']['api']['bluesun']

    with patch.object(WrenchAuthWrapper, 'get_connection_info', return_value=rv):
        with patch.object(requests, 'post', return_value=mock_response):
            with patch.object(Response, 'json', return_value={}):
                api_key = new_auth_instance.get_api_key('bluesun')
                assert 'host' in api_key


def test_uploader_upload_params(future_api_key):
    file_path = '/tmp/foo.csv.gz'
    upload_file_name = 'foo.csv'
    with patch.object(WrenchUploader, 'upload') as upload_mock:
        WrenchUploader.upload(future_api_key, file_path, upload_file_name)
    upload_mock.assert_called_once_with(future_api_key, file_path, upload_file_name)


def test_uploader_upload_response(future_api_key):
    file_path = '/tmp/foo.csv.gz'
    upload_file_name = 'foo.csv'
    wrench_file_name = f'20201207122500-{upload_file_name}'
    mock_response = Response()
    mock_response.status_code = 200
    mock_response.json = {
        'message': f'File {wrench_file_name} Uploaded Successfully',
        'file': f'{wrench_file_name}'
    }

    with patch.object(gzip, 'open'):
        with patch.object(requests, 'put', return_value=mock_response):
            response = WrenchUploader.upload(future_api_key, file_path, upload_file_name)
            assert response == mock_response
