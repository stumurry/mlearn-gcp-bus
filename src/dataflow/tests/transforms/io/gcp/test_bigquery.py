import pytest
import apache_beam as beam
from libs import GCLOUD as gcloud
from transforms.io.gcp import bigquery as bq
from transforms.io.gcp.bigquery import WriteToBigQueryTransform
from google.cloud import bigquery as gcp_bq
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from libs.shared.test_factories import FactoryRegistry
from apache_beam.options.value_provider import RuntimeValueProvider
from google.cloud.bigquery import Client
from google.cloud.bigquery.job import LoadJobConfig, LoadJob
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from unittest import mock
from apache_beam.options.pipeline_options import PipelineOptions


def test_empty_strings():
    ls = ['project', 'table', 'query']
    fn = bq.GetBigQueryDataFn(**{n: '' for n in ls})
    for f in ls:
        attr = getattr(fn, f'_{f}')
        assert attr.get() == ''


def test_page_size():
    fn = bq.GetBigQueryDataFn(page_size=100)
    assert fn._page_size.get() == 100


def test_valid_params():
    ls = ['project', 'table', 'query']
    fn = bq.GetBigQueryDataFn(**{n: 'test' for n in ls})
    for f in ls:
        attr = getattr(fn, f'_{f}')
        assert attr.get() == 'test'


@pytest.fixture(scope='module')
def seed(bigquery):
    seeds = [('lake', [
        ('tree_user_types', []),
        ('pyr_rank_definitions',
         FactoryRegistry.registry['LakePyrRankDefinitionFactory'])])]
    bigquery.truncate(seeds)
    bigquery.seed(seeds)


def test_query_not_env():
    with pytest.raises(Exception) as exception:
        with TestPipeline() as p:
            (p | 'No env for Query' >> bq.ReadAsJson(query='Test'))
    assert 'Must specify env param' == str(exception.value)


def test_query_and_table():
    with pytest.raises(Exception) as exception:
        with TestPipeline() as p:
            (p | 'Query and Table Set' >> bq.ReadAsJson(
                                            query='Test',
                                            table='Test2'))
    assert 'Cannot use both table and query arguments!' == str(exception.value)


def test_not_query_and_not_table():
    with pytest.raises(Exception) as exception:
        with TestPipeline() as p:
            (p | 'Query and Table Set' >> bq.ReadAsJson(
                                            query=None,
                                            table=None))
    assert 'Must specify query or full table path' == str(exception.value)


def test_table_read_not_project(seed, env):
    with TestPipeline() as p:
        pcoll = (p | 'Reading from table' >> bq.ReadAsJson(
                        table=f'{env["project"]}.lake.pyr_rank_definitions',
                        env=env["env"])
                   | 'Get Rows Count' >> beam.combiners.Count.Globally())
        assert_that(pcoll, equal_to([16]))


def test_dest_table_schema(env):
    project_id = gcloud.project(env['env'])
    with TestPipeline() as p:
        lake_table = f'{project_id}:lake.wrench_metrics'
        expected = [
            {
                'schema': [
                    gcp_bq.schema.SchemaField('icentris_client', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('client_wrench_id', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('entity_id', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('contact_id', 'INTEGER',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('owner_tree_user_id', 'INTEGER',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('processing_datetime',
                                              'DATETIME', 'NULLABLE',
                                              None, ()),
                    gcp_bq.schema.SchemaField('metrics',
                                              'RECORD',
                                              'REPEATED',
                                              fields=[
                                                  gcp_bq.schema.SchemaField('name', 'STRING', 'REQUIRED'),
                                                  gcp_bq.schema.SchemaField('value', 'STRING')
                                              ]),
                    gcp_bq.schema.SchemaField('ingestion_timestamp',
                                              'TIMESTAMP', 'REQUIRED',
                                              None, ())
                ],
                'payload':{}
            }]
        pcoll = p | beam.Create([{}])
        schema_pcoll = pcoll | beam.ParDo(
            bq.InjectTableSchema(table=lake_table, env=env['env']))
        assert_that(schema_pcoll, equal_to(expected))


def test_dest_table_schema_runtime(env):
    project_id = gcloud.project(env['env'])
    with TestPipeline() as p:
        lake_table = RuntimeValueProvider(
            option_name='dest',
            value_type=str,
            default_value=f'{project_id}:lake.wrench_metrics'
        )
        expected = [
            {
                'schema': [
                    gcp_bq.schema.SchemaField('icentris_client', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('client_wrench_id', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('entity_id', 'STRING',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('contact_id', 'INTEGER',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('owner_tree_user_id', 'INTEGER',
                                              'REQUIRED', None, ()),
                    gcp_bq.schema.SchemaField('processing_datetime',
                                              'DATETIME', 'NULLABLE',
                                              None, ()),
                    gcp_bq.schema.SchemaField('metrics',
                                              'RECORD',
                                              'REPEATED',
                                              fields=[
                                                  gcp_bq.schema.SchemaField('name', 'STRING', 'REQUIRED'),
                                                  gcp_bq.schema.SchemaField('value', 'STRING')
                                              ]),
                    gcp_bq.schema.SchemaField('ingestion_timestamp',
                                              'TIMESTAMP', 'REQUIRED',
                                              None, ())
                ],
                'payload':{}
            }]
        pcoll = p | beam.Create([{}])
        schema_pcoll = pcoll | beam.ParDo(
            bq.InjectTableSchema(table=lake_table, env=env))
        assert_that(schema_pcoll, equal_to(expected))
        RuntimeValueProvider.set_runtime_options(None)


def test_write_to_bigquery(env):
    client = mock.create_autospec(Client)
    load_job = mock.create_autospec(LoadJob)
    job_config = mock.create_autospec(LoadJobConfig)
    table = mock.create_autospec(Table)

    client.load_table_from_json.return_value = load_job
    client.get_table.return_value = table

    table_name = 'icentris-ml-prd:lake.tree_user_statuses'
    expected_table_name = 'icentris-ml-prd.lake.tree_user_statuses'
    element = 'test_payload'
    fn = bq.WriteToBigQuery(table=table_name, env=env['env'])
    fn.client = lambda env: client
    fn.load_job_config = lambda: job_config
    fn.process(element=element)
    client.load_table_from_json.assert_called_once_with(element, expected_table_name, job_config=job_config)
    load_job.result.assert_called_once()
    client.get_table.assert_called_once_with(expected_table_name)
    assert job_config.schema == table.schema
    assert job_config.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON


def test_write_to_bigquery_transform():
    side_effect = [{'a': 1, 'b': 2}]

    class MockOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_value_provider_argument('--dest', default='dataset:table',
                                               help='Table to write to. Ex: project_id:lake.pyr')

    options = MockOptions()

    def test_dev():

        with TestPipeline() as p:
            write_to_bigquery_transform = WriteToBigQueryTransform(table=options.dest, env='dev')
            write_to_bigquery_transform._write_to_bigquery.process = lambda x: [(yield x)]
            result = (p | 'Create fake Bigquery Data' >> beam.Create(side_effect)
                      | 'Test WriteToBigQueryTransform' >> write_to_bigquery_transform)
            assert_that(result, equal_to([side_effect]))

    def test_prd():
        with TestPipeline() as p:
            write_to_bigquery_transform = WriteToBigQueryTransform(table=options.dest, env='prd')
            write_to_bigquery_transform._write_to_bigquery.expand = lambda pcoll: pcoll
            result = (p | 'Create fake Bigquery Data' >> beam.Create(side_effect)
                        | 'Test WriteToBigQueryTransform' >> write_to_bigquery_transform)
            assert_that(result, equal_to(side_effect))

    test_dev()
    test_prd()
