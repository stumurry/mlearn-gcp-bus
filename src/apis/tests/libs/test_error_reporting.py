from flask_restful_swagger_3 import Resource
from libs.response_helper import ResponseHelper, handle_exception, response_to_json, ignored_response_codes
from unittest.mock import patch
from flask_jwt_extended import jwt_required


class MockedException(Resource):
    @handle_exception
    def get(self):
        raise Exception('mocked exception')


class MockedAbort(Resource):
    def get(self, code):
        return ResponseHelper.abort(code, msg='mocked abort')


class MockedJWTRequired(Resource):
    @jwt_required
    def get(self):
        return 'ok', 200


@patch('libs.email_helper.EmailHelper.report_issue')
def test_report_errors(report_issue, server, test_client):

    server.api.add_resource(MockedAbort, '/abort/<int:code>')
    server.api.add_resource(MockedException, '/exception')
    server.api.add_resource(MockedJWTRequired, '/jwt-required')

    for i in ignored_response_codes:
        resp = test_client.get(f'/abort/{i}')
        assert resp.status_code == i
        assert response_to_json(resp)['msg'] == 'mocked abort'
        assert not report_issue.called  # We don't need to be notified everytime they can't login

    resp = test_client.get('/abort/500')
    assert resp.status_code == 500
    assert response_to_json(resp)['msg'] == 'mocked abort'
    report_issue.assert_called_with('mocked abort', subject='Recommendation API')

    resp = test_client.get('/exception')
    assert resp.status_code == 500
    assert response_to_json(resp)['msg'] == 'mocked exception'
    report_issue.assert_called_with('mocked exception', subject='Recommendation API')

    resp = test_client.get('/jwt-required')
    assert response_to_json(resp)['msg'] == 'Missing Authorization Header'
    assert resp.status_code == 401


@patch('libs.email_helper.EmailHelper.report_issue')
def test_abort(report_issue):
    response = ResponseHelper.abort(400, asdf='missing msg argument')
    assert str(type(response)) == "<class 'flask.wrappers.Response'>"
    assert response_to_json(response) == {'msg': 'Abort() missing msg argument'}
    response = ResponseHelper.abort(401, msg='Good Message')
    assert response_to_json(response) == {'msg': 'Good Message'}
