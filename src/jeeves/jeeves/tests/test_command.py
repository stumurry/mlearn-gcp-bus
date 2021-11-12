import pytest
from jeeves.commands import crypto
from unittest.mock import patch
from jeeves.commands.libs import Config


@patch('jeeves.commands.crypto.encode_secret', return_value='asdf')
def test_crypto_create_secret(encode_secret):
    with patch.object(Config, 'add_secret_version'):
        with pytest.raises(SystemExit):
            try:
                crypto.create_secret(args=['local', 'location', 'keyring', 'keyname', 'secret', 'plaintext'])
            finally:
                Config.add_secret_version.assert_called_with('local', 'secret', 'asdf')
