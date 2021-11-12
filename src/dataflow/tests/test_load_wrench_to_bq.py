import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from load_wrench_to_bq import (
    ReadWrenchInsights,
    MatchScoreTransform,
    AdoptCurveTransform
)
from unittest.mock import patch
import requests
from libs import WrenchAuthWrapper
import pytest


@pytest.fixture
def mock_api_key():
    return {
        "host": 'http://mock-url/',
        "api_key": 'mock_api_key'
    }


@pytest.fixture
def mock_insight():
    return dict(scores=[dict(entity_id='3c711ca6', plexus=50)])


@pytest.fixture()
def run_test_pipeline(env, mock_api_key, mock_insight):

    def _test(insight_id, mock_response):
        with patch.object(WrenchAuthWrapper, 'get_api_key', return_value=mock_api_key):
            with patch.object(requests, 'post', return_value=mock_response):
                with patch.object(mock_response, 'json', return_value=mock_insight):
                    with TestPipeline() as p:
                        input = (1, [{"entity_id": "3c711ca6"}, {"entity_id": "4c711ca5"}])
                        result = (
                            p
                            | beam.Create([input])
                            | beam.ParDo(ReadWrenchInsights(env=env['env'], insight_id=insight_id))
                        )
        return result
    return _test


def test_read_wrench_insights(run_test_pipeline):
    mock_response = requests.Response()
    mock_response.status_code = requests.codes.ok
    expected = expected = [dict(entity_id='3c711ca6', plexus=50)]
    result = run_test_pipeline('match-score', mock_response)
    assert_that(result, equal_to(expected))


def test_read_wrench_insights_status_102_ignored(run_test_pipeline):
    mock_response = requests.Response()
    mock_response.status_code = 102
    expected = expected = []
    result = run_test_pipeline('match-score', mock_response)
    assert_that(result, equal_to(expected))


def test_read_wrench_insights_status_204_ignored(run_test_pipeline):
    mock_response = requests.Response()
    mock_response.status_code = 204
    expected = expected = []
    result = run_test_pipeline('match-score', mock_response)
    assert_that(result, equal_to(expected))


def test_read_wrench_insights_status_402_ignored(run_test_pipeline):
    mock_response = requests.Response()
    mock_response.status_code = 402

    expected = expected = []
    result = run_test_pipeline('match-score', mock_response)
    assert_that(result, equal_to(expected))


def test_match_score_transform():
    inputs = [{
        'entity_id': '3c711ca6',
        'bluesun': 30,
        'plexus': 60
    }]
    with TestPipeline() as p:
        scores = [
            {'icentris_client': 'bluesun', 'score': 30},
            {'icentris_client': 'plexus', 'score': 60}
        ]
        expected = [{'entity_id': '3c711ca6', 'scores': scores}]
        result = (
            p
            | 'Create insights' >> beam.Create(inputs)
            | 'Test MatchScoreTransform' >> beam.ParDo(MatchScoreTransform(insight_id='match-score'))
        )
        assert_that(result, equal_to(expected))


def test_adopt_curve_transform():
    inputs = [{
        'entity_id': '3c711ca6',
        'direct_sales_2 Persona': 'late_adopter',
        'monat_2 Persona': 'late_adopter'
    }]
    with TestPipeline() as p:
        scores = [
            {'corpus': 'direct_sales_2 Persona', 'score': 'late_adopter'},
            {'corpus': 'monat_2 Persona', 'score': 'late_adopter'}
        ]
        expected = [{'entity_id': '3c711ca6', 'scores': scores}]
        result = (
            p
            | 'Create insights' >> beam.Create(inputs)
            | 'Test AdoptCurveTransform' >> beam.ParDo(AdoptCurveTransform(insight_id='adopt-curve'))
        )
        assert_that(result, equal_to(expected))
