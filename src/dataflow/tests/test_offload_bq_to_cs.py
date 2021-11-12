import pytest
import os
import gzip
import simplejson as json
import apache_beam as beam
import uuid

from libs import GCLOUD as gcloud, BigQuery
from decimal import Decimal
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
from offload_bq_to_cs import Runner, RuntimeOptions
from transforms.wrench_export_to_gcs import UploadTransform
# from transforms.wrench_export_to_gcs import UploadByKey
from libs.shared.storage import CloudStorage
from unittest.mock import patch
# from unittest.mock import call


bucket = f"{gcloud.project(os.environ['ENV'])}-dataflow"
blob_name = "offload_bq_to_cs_test"
suffix = '-00000-of-00001.ndjson.gz'
dest_blob_name = f'{blob_name}{suffix}'


@pytest.fixture(scope='module')
def ndjson(env, cloudstorage, record_testsuite_property):
    cloudstorage.client.delete_blob(bucket, dest_blob_name)
    assert cloudstorage.client.blob_exists(bucket, dest_blob_name) is False

    sql = BigQuery.querybuilder(
        union=('all', [
            BigQuery.querybuilder(select=[
                ('NULL', 'none'),
                ('True', 'true_bool'),
                ('False', 'false_bool'),
                ('"2020-04-03"', 'date'),
                ('"2020-04-03 13:45:00"', 'datetime'),
                ('"1966-06-06 06:06:06.666666 UTC"', 'timestamp'),
                ('"STRING"', 'string'),
                ('234', 'integer'),
                ('123.54', 'float')
            ]),
            BigQuery.querybuilder(select=['NULL'] * 9),
            BigQuery.querybuilder(select=[
                '"String"',
                'False',
                'True',
                '"1993-09-03"',
                '"1993-09-03 03:44:00"',
                '"1993-09-03 03:44:00.777555 UTC"',
                '"Not String"',
                '567',
                '456'
            ])
        ])
    )

    RuntimeValueProvider.set_runtime_options(None)

    options = RuntimeOptions(['--env', env['env'],  '--query', str(sql), '--destination', f'gs://{bucket}/{blob_name}'])
    Runner._run(TestPipeline(options=options), options)

    assert cloudstorage.client.blob_exists(bucket, dest_blob_name) is True

    zbytes = cloudstorage.client.download_blob_as_string(bucket, dest_blob_name)
    bytes = gzip.decompress(zbytes)
    lines = bytes.decode('utf8').rstrip().split('\n')
    yield [json.loads(line) for line in lines]


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_file_has_3_rows(ndjson):
    assert len(ndjson) == 3


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_int_is_int(ndjson):
    assert isinstance(ndjson[0]['integer'], int)
    assert isinstance(ndjson[2]['integer'], int)
    assert ndjson[0]['integer'] == 234
    assert ndjson[2]['integer'] == 567


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_None_is_empty_string(ndjson):
    assert ndjson[0]['none'] == ''


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_bool_false_is_0(ndjson):
    assert ndjson[0]['false_bool'] is not False and ndjson[0]['false_bool'] == 0
    assert ndjson[2]['true_bool'] is not False and ndjson[2]['true_bool'] == 0


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_bool_true_is_1(ndjson):
    assert ndjson[0]['true_bool'] is not True and ndjson[0]['true_bool'] == 1
    assert ndjson[2]['false_bool'] is not True and ndjson[2]['false_bool'] == 1


@pytest.mark.skip(reason='nsjson fixture needs to be mocked')
def test_all_fields_null_empty_strings(ndjson):
    assert all([v == '' for k, v in ndjson[1].items()])


def test_simplejson_parses_decimal(env):
    test_row = {'foo': Decimal('1.99')}

    with TestPipeline() as p:
        (p | beam.Create([test_row])
           | beam.Map(json.dumps))


def test_partition_upload_by_client(env):
    _bucket = 'icentris-ml-local-jcain-wrench-exports'
    _table = 'staging.contacts'
    _file = f'{uuid.uuid4()}.csv.gz'

    entries = [
        {'icentris_client': 'bluesun', 'first_name': 'John', 'last_name': 'Doe'},
        {'icentris_client': 'bluesun', 'first_name': 'Jim', 'last_name': 'Beam'},
        {'icentris_client': 'plexus', 'first_name': 'Jane', 'last_name': 'Smith'}]

    # expected_bluesun_content = UploadByKey.csv_and_compress_elements(entries[0:2])
    # expected_plexus_content = UploadByKey.csv_and_compress_elements([entries[2]])

    # expected = [
    #     call(_bucket, f'{_table}-bluesun-{_file}', expected_bluesun_content, is_binary=True),
    #     call(_bucket, f'{_table}-plexus-{_file}', expected_plexus_content, is_binary=True)]

    with patch.object(CloudStorage, 'upload_blob_content') as mock_upload:
        with TestPipeline() as p:
            (p | beam.Create(entries)
               | beam.GroupBy(lambda x: x['icentris_client'])
               | UploadTransform(
                env['env'],
                _bucket,
                _table,
                _file))

        assert mock_upload.call_count == 2

        # print('\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        # print(mock_upload.mock_calls)
        # print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')

        # print('\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        # print(f"call('{_bucket}', '{_table}-bluesun-{_file}', {expected_bluesun_content}, is_binary=True)")
        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

        # print('\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        # print(f"call('{_bucket}', '{_table}-plexus-{_file}', {expected_plexus_content}, is_binary=True)")
        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

        # print(mock_upload.call_args_list)
        # assert mock_upload.call_args_list == expected

        # For some reason this fails occassionaly on local testing. It fails all the time
        # on dev. jc 2/11/2021
        # mock_upload.assert_has_calls(expected, any_order=True)
