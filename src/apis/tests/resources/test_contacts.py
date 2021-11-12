from libs.response_helper import response_to_json
from unittest.mock import patch
from services.contacts_service import ContactsService
from models.contacts_model import GetContactsResponse

expected = {'contacts': [
    {'id': 1},
    {'id': 2},
    {'id': 3},
]}


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_get_contacts(mock_jwt_required, mock_auth, auth_client, test_client):
    expected_response = GetContactsResponse(**expected)
    with patch.object(ContactsService, 'get_contacts', return_value=expected_response):
        data = {
            'contact_ids': '1,2,3'
        }
        response = test_client.get('/contacts', query_string=data)
        assert expected_response == response_to_json(response)


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
@patch('libs.email_helper.EmailHelper.report_issue')
def test_get_resource_no_longer_exists(report_issue, mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'get_contacts', return_value=None):
        response = test_client.get('/contacts')
        assert response_to_json(response) == {'msg': 'contact_ids is malformed or missing'}
        assert response.status_code == 400
        assert not report_issue.called


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
@patch('libs.email_helper.EmailHelper.report_issue')
def test_get_validate(report_issue, mock_jwt_required, mock_auth, auth_client, test_client):
    expected_response = GetContactsResponse(**expected)
    with patch.object(ContactsService, 'get_contacts', return_value=expected_response):
        data = {
            'contact_ids': '1,2,Hello,World,3'
        }
        response = test_client.get('/contacts', query_string=data)
        assert response_to_json(response) == {'msg': 'contact_ids is malformed or missing'}
        assert response.status_code == 400
        assert not report_issue.called

        data = {
            'contact_ids': '1,2,3,'
        }
        response = test_client.get('/contacts', query_string=data)
        assert expected_response == response_to_json(response)


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        data = {'contacts': [
            {'id': 1, 'first_name': 'John', 'last_name': 'Doe'},
            {'id': 2, 'first_name': 'John', 'last_name': 'Doe', 'city': 'Schenectady'}]}
        response = test_client.post('/contacts', json=data)
        assert response.status_code == 200


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_missing_required_returns_400(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        # No id which is a required field
        data = {'contacts': [{'first_name': 'John', 'last_name': 'Doe'}]}
        response = test_client.post('/contacts', json=data)
        assert response.status_code == 400


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_extra_field_returns_400(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        data = {'contacts': [{'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'foo': 'bar'}]}
        response = test_client.post('/contacts', json=data)
        assert response.status_code == 400


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_missing_payload_returns_400(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        response = test_client.post('/contacts')
        assert response.status_code == 400


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_missing_key_returns_400(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        response = test_client.post('/contacts', json={})
        assert response.status_code == 400


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_malformed_payload_returns_400(mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(ContactsService, 'post_contacts'):
        response = test_client.post('/contacts', json='{asdf}')
        assert response.status_code == 400


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
def test_post_contacts_enriches_payload(mock_jwt_required, mock_auth, auth_client, test_client):
    data = {'contacts': [{'id': 1, 'first_name': 'John', 'last_name': 'Doe'}]}
    with patch.object(ContactsService, 'post_contacts') as service:
        test_client.post('/contacts', json=data)

    assert 'icentris_client' in service.call_args[0][0][0]
    assert 'ingestion_timestamp' in service.call_args[0][0][0]
