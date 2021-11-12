import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from transforms.decimal import StringifyDecimal
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import decimal
import json


def test_StringifyDecimal():
    with TestPipeline() as p:
        d = decimal.Decimal('5.5')
        payload = {'decimal': str(d), 'arr': [{'decimal': str(d)}]}
        expected_payload = '{"decimal": "5.5", "arr": [{"decimal": "5.5"}]}'
        pcoll = p | beam.Create([payload])
        date_coll = (pcoll | beam.ParDo(StringifyDecimal())
                     | 'Test JSON parser' >> beam.Map(json.dumps))
        assert_that(date_coll, equal_to([expected_payload]), label='assert:decimal')
