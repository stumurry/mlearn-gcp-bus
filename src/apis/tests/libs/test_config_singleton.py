import pytest
from unittest.mock import patch
from libs.config_singleton import ConfigSingleton
from libs.shared.config import Config


@pytest.fixture(scope='module')
def mocked_config():
    return {'foo': 'bar', 'buzz': 'baz'}


def test_is_singleton():
    a = ConfigSingleton.config_instance()
    b = ConfigSingleton.config_instance()
    assert id(a) == id(b)


def test_exception():
    with patch.object(ConfigSingleton, 'config_instance', side_effect=Exception('forced exception')):
        with pytest.raises(Exception):
            c = ConfigSingleton()
            c.get_config('local')


def test_gets_config(mocked_config):
    with patch.object(Config, 'get_config', return_value=mocked_config):
        config = ConfigSingleton().get_config('local')
        assert config == mocked_config


def test_get_cache_called(mocked_config):
    with patch.object(Config, 'get_cache', return_value=mocked_config):
        ConfigSingleton().get_config('local')
        Config.get_cache.assert_called_once_with('local')


def test_set_cache_called(mocked_config):
    with patch.object(Config, 'get_cache', return_value=None):
        with patch.object(Config, 'get_config', return_value=mocked_config):
            with patch.object(Config, 'set_cache'):
                ConfigSingleton().get_config('local')
                Config.set_cache.assert_called_once_with('local', mocked_config)


def test_clear_cache(mocked_config):
    cache_key = 'foo'
    cache_value = 'bar'
    # Set an item in cache
    ConfigSingleton.config_instance().set_cache(cache_key, cache_value)
    # Verify item was cached
    cached = ConfigSingleton.config_instance().get_cache(cache_key)
    assert cached == cache_value
    # Clear config
    ConfigSingleton.config_instance().clear_cache()
    # Verify item no longer exists
    cached = ConfigSingleton.config_instance().get_cache(cache_key)
    assert cached is None
