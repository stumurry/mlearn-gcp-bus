from .config import Config
import requests
from datetime import datetime
import logging
import json
import gzip


class WrenchAuthWrapper():
    log = logging.getLogger()
    me = None

    def __init__(self, env):
        self.env = env
        self.api_keys = {}

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = cls(env)

        return cls.me

    def get_connection_info(self, icentris_client):
        c = Config.get_config(self.env)

        wrench = c.get('wrench', None)
        if wrench is None:
            raise KeyError('wrench key not found in config')

        api = wrench.get('api', None)
        if api is None:
            raise KeyError('api key not found in wrench config')

        client = api.get(icentris_client, None)
        if client is None:
            raise KeyError(f'{icentris_client} not found in api config')

        return client

    def get_api_key(self, icentris_client):
        if self.is_expired(icentris_client):
            self.log.debug('api_key does not exist or is expired')
            ci = self.get_connection_info(icentris_client)

            self.log.debug('Asking Wrench for a new api_key')
            response = requests.post(
                f"{ci['host']}/generate-api-key",
                data=json.dumps({'username': ci['username'], 'password': ci['password']}),
                headers={'content-type': 'application/json'})

            if response.status_code == 200:
                self.log.debug('Wrench call successful')
                self.api_keys[icentris_client] = response.json()
                self.api_keys[icentris_client]['host'] = ci['host']
            else:
                self.log.debug(f'Wrench call failed with status {response.status_code}')
                response.raise_for_status()

        return self.api_keys[icentris_client]

    def is_expired(self, icentris_client):
        if icentris_client not in self.api_keys:
            self.log.debug('icentris_client not in api_key, treat it as expired')
            return True

        if self.api_keys[icentris_client].get('expiry_time', None) is None:
            self.log.debug('expiry_time does not exist, treat it as expired')
            return True

        try:
            parsed_datetime = self.truncate_nanoseconds(self.api_keys[icentris_client]['expiry_time'])
            et = datetime.strptime(parsed_datetime, '%Y-%m-%d %H:%M:%S.%f %z %Z')
            ct = datetime.now(et.tzinfo)
            expired = et < ct
            self.log.debug('api_key is expired')
            return expired
        except ValueError as e:
            self.log.debug(f'Error parsing expiry_time: {e}')
            return True

    def clear(self):
        self.api_keys = {}

    def truncate_nanoseconds(self, ds):
        # Wrench returns a datetime with nanoseconds.
        # datetime.strptime() doesn't support that so we have to truncate the last 3 digits.
        # See tests for format examples. jc 11/19/2020
        try:
            datetime_parts = ds.split('.')
            ns_tz_parts = datetime_parts[1].split(' ')
            return ''.join([datetime_parts[0], '.', ns_tz_parts[0][:-3], ' ', ns_tz_parts[1], ' ', ns_tz_parts[2]])
        except Exception as e:
            self.log.debug(f'Error parsing datetime string: {e}')

        return ds


class WrenchUploader():
    def upload(api_key, file_path, upload_file_name):
        files = {'file': (upload_file_name, gzip.open(file_path, 'rb'), 'multipart/form-data')}
        response = requests.put(
            f"{api_key['host']}/upload",
            files=files,
            headers={
                'authorization': api_key['api_key']
            })
        return response
