from libs.response_helper import response_to_json
from unittest.mock import patch
from services.leads_service import LeadsService
from models.leads_model import LeadsModel
# import pytest

expected = {'leads': [
    {'lead_id': 1},
    {'lead_id': 2},
    {'lead_id': 3},
]}


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
# @pytest.mark.skip(reason='Works, bigquery multithreading is corrupting seed data.')
def test_get_leads(mock_jwt_required, mock_auth, auth_client, test_client):

    expected_response = LeadsModel(**expected)
    with patch.object(LeadsService, 'get_leads', return_value=expected_response):
        data = {
            'lead_ids': '1,2,3'
        }
        response = test_client.get('/leads', query_string=data)
        assert expected_response == response_to_json(response)


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
@patch('libs.email_helper.EmailHelper.report_issue')
def test_get_resource_no_longer_exists(report_issue, mock_jwt_required, mock_auth, auth_client, test_client):
    with patch.object(LeadsService, 'get_leads', return_value=None):
        response = test_client.get('/leads')
        assert response_to_json(response) == {'msg': 'lead_ids is malformed or missing'}
        assert response.status_code == 400
        assert not report_issue.called


@patch('resources.auth.Auth.get_auth_icentris_client', return_value='bluesun')
@patch('resources.auth.Auth.validate_access_token_identity', return_value=True)
@patch('flask_jwt_extended.view_decorators.verify_jwt_in_request')
@patch('libs.email_helper.EmailHelper.report_issue')
def test_get_validate(report_issue, mock_jwt_required, mock_auth, auth_client, test_client):
    expected_response = LeadsModel(**expected)
    with patch.object(LeadsService, 'get_leads', return_value=expected_response):
        data = {
            'lead_ids': '1,2,Hello,World,3'
        }
        response = test_client.get('/leads', query_string=data)
        assert response_to_json(response) == {'msg': 'lead_ids is malformed or missing'}
        assert response.status_code == 400
        assert not report_issue.called

        data = {
            'lead_ids': ''
        }
        response = test_client.get('/leads', query_string=data)
        assert response_to_json(response) == {'msg': 'lead_ids is malformed or missing'}
        assert response.status_code == 400
        assert not report_issue.called

        data = {
            'lead_ids': '1,2,3,'
        }
        response = test_client.get('/leads', query_string=data)
        assert expected_response == response_to_json(response)
