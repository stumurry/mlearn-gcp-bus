import jwt
import requests
from libs.config_singleton import ConfigSingleton
from services import test_contacts_service
from services import test_leads_service
from models.contacts_model import GetContactsResponse
from models.leads_model import LeadsModel
import os
import pytest

domain_url = 'bluesun.dev.ml-apis.vibeoffice.com'


@pytest.fixture()
def should_test_api():
    is_dev = os.environ.get('ENV', 'dev') == 'dev'
    is_test_after_deployment = os.environ.get('TEST_AFTER_DEPLOYMENT') == 'true'
    if (is_dev and is_test_after_deployment):
        return True
    else:
        return False


def auth(domain):
    key = ConfigSingleton.config_instance().get_config('dev')
    domain = key['api']['domains'][domain]
    api_key = key['api']['api_keys'][domain]

    payload = {
        'sub': api_key['client'],
        'iat': '1516239022',
        # 'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    }

    secret_key = api_key['key']
    token = jwt.encode(payload, secret_key, algorithm='HS256').decode("utf-8")

    data = {
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': token
    }

    print('domain', domain_url)
    x = requests.post(f'https://{domain_url}/auth', data=data)

    return x.text.replace('"', '').replace('\n', '')


def get_headers():
    bearer_token = auth(domain_url)
    return {
        'Authorization': f'Bearer {bearer_token}'
    }


def test_contacts(env, bigquery, should_test_api):
    # should_test_api is a fixture instead of wrapper because CICD caches the wrapper and it needs to be variable.
    if (should_test_api):
        seeds = test_contacts_service._seeds()
        bigquery.truncate(seeds)
        bigquery.seed(seeds)

        response = requests.get(f'https://{domain_url}/contacts?contact_ids=1', headers=get_headers())
        expected_response = GetContactsResponse(**test_contacts_service.expected_contacts)
        assert response.json() == expected_response


def test_leads(env, bigquery, should_test_api):
    if (should_test_api):
        seeds = test_leads_service._seeds()
        bigquery.truncate(seeds)
        bigquery.seed(seeds)

        response = requests.get(f'https://{domain_url}/leads?lead_ids=1', headers=get_headers())
        expected_response = LeadsModel(**test_leads_service.expected_leads)
        assert response.json() == expected_response
