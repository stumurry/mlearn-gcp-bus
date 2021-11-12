import pytest
from .crypto import Crypto
import json


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def test_decrypt():
    good_key = '6L53g000h6WjSsuzTb3pE4yp7LzueVViIw6enaktLPM='
    bad_key = 'A REALLY REALLY MESSED UP KEY'
    payload = {
        'id': 1, 'category_name': 'phone', 'user_id': None, 'status': None, 'created_at': '2019-01-26T17:34:26.000Z',
        'updated_at': '2020-04-02T09:32:47.000Z', 'contacts_count': 1, 'icentris_client': 'foo_client'
    }
    keys = [
        bad_key,
        good_key
    ]
    enc = Crypto.encrypt(raw=json.dumps(payload), key=good_key)  # `good_key` called
    dec = json.loads(Crypto.decrypt(enc=enc, keys=keys))  # `bad_key` then `good_key` called
    assert payload == dec


@pytest.mark.skip(reason='This is a test you can run on demand against a local file')
def test_decrypt_file(env):
    crypto = Crypto(env=env['env'], key='vibe-cdc')
    keys = ['']

    with open('./test_decrypt_leo_file') as file:
        for line in file:
            print('Decrypt Line')
            print(crypto.decrypt(line, keys))
