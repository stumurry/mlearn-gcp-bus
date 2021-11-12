import json
import apache_beam as beam
from libs import Crypto
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from load_cdc_from_gcs_to_lake import RecordDecryption, Runner, RuntimeOptions
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakePyrMessageRecipientsFactory
from unittest.mock import patch
from libs.shared.test import skipif_prd
import pytest

lake_table = 'lake.pyr_message_recipients'


@skipif_prd
@pytest.mark.skip(reason='Skipping for now.')
def test_end_to_end(env, bigquery):
    lake_table = 'lake.pyr_message_recipients'
    files_startwith = 'vibe-message-recipients-final'
    start = '2020-06-01 12:00:00 UTC'

    FactoryRegistry.create_multiple(LakePyrMessageRecipientsFactory, 1, [
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': start}
    ])

    seeds = [
        ('lake', [
            ('pyr_message_recipients', FactoryRegistry.registry['LakePyrMessageRecipientsFactory'])
        ])]

    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    RuntimeValueProvider.set_runtime_options(None)
    print('Running pipeline.')
    options = RuntimeOptions([
        '--env',
        env['env'],
        '--files_startwith',
        files_startwith,
        '--dest',
        lake_table])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)

    rs = bigquery.query(f'select * from {lake_table} ')

    assert len(rs) == 1


def test_record_decryption_can_decrypt(env):
    file_seed = 'b5a6a555c2ceff9d90655e83a989491e:\
    e6fc7acd34f90fffafb2e1707ff3d801c954db15e4688b066108e47ef1\
    ff8b01c371f1db51b4fb330f759f56acf8c86324d73ee6ca343f866c70f\
    b4e61684257851f0ff7f956a70ecc8a61a59ba73d17504468bc956fa669\
    a907b2ed85bc7f5e3d394f160810cb3a5417ae7a1342317da3a0ac7567f\
    6adc6286017d68444823b0246fa70f8b3c60d32a748d7a92823813beb53\
    179419eb91634a88bcfbaa49b7f7f34b597e762779135eda7dd30eb600'
    expected = {
        'id': 1, 'category_name': 'phone', 'user_id': None,
        'status': None, 'created_at': '2019-01-26T17:34:26.000Z',
        'updated_at': '2020-04-02T09:32:47.000Z', 'contacts_count': 8629539,
        'icentris_client': 'worldventures'
    }

    with patch.object(Crypto, 'decrypt', return_value='encryptedlineplaceholder'):
        with patch.object(json, 'loads', return_value=expected):
            RecordDecryption.get_keys = lambda self: ['6L53g000h6WjSsuzTb3pE4yp7LzueVViIw6enaktLPM=']
            with TestPipeline() as p:
                pcoll = (
                    p | beam.Create([file_seed])
                    | beam.ParDo(RecordDecryption(env=env['env']))
                )
                assert_that(pcoll, equal_to([expected]))
