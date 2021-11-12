import logging
import pytest
import os
from libs import GCLOUD as gcloud
from libs.shared.test import BigQueryFixture
from libs.shared.test import CloudStorageFixture
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider


log = logging.getLogger()


@pytest.fixture(scope='session')
def sql_templates_path():
    return '/workspace/dataflow/tests/sql_templates'


@pytest.fixture(scope='module')
def bigquery(env):
    helper = BigQueryFixture.instance(env['env'])
    return helper


@pytest.fixture(scope='session')
def cloudstorage(env):
    return CloudStorageFixture.instance(env['env'])


@pytest.fixture(scope='session')
def env():
    return {
        'env': os.environ['ENV'],
        'project': gcloud.project(os.environ['ENV']),
        'user': gcloud.config()['username'],
        'client': 'bluesun'
    }

# Keep this snippet for later, just in case we want to revert back to streaming - Stu M 7/4/20
# class WriteToFile(beam.DoFn):
#     def __init__(self, **kwargs):
#         for k, v in kwargs.items():
#             setattr(self, '_' + k, v)

#     def process(self, unused_el):
#         if isinstance(self._dest, ValueProvider):
#             self._dest = self._dest.get()
#         with open(f'/workspace/dataflow/__tmp__{self._dest}__tmp__.txt', 'a') as f:
#             f.write(f'{unused_el}\n')
#             yield unused_el


@pytest.fixture(scope='module')
def run_pipeline(bigquery):
    def _r(runner, options, seeds):
        bigquery.truncate(seeds)
        bigquery.seed(seeds)

        RuntimeValueProvider.set_runtime_options(None)
        print('Running pipeline.')
        pipeline = TestPipeline(options=options)
        runner._run(pipeline, options)

        # Keep this snippet for later, just in case we want need to revert back to streaming - Stu M 7/4/20

        # def wraps():
        #     return ('FormatOutput' >> beam.Map(json.dumps)
        #             | 'WriteToFile' >> beam.ParDo(WriteToFile(dest=options.dest)))

        # with patch.object(helper, 'write_to_bigquery', wraps=wraps) as w:
        #     print('Running PIPELINE')
        #     pipeline = TestPipeline(options=options)
        #     runner._run(pipeline, options)
        #     path = '/workspace/dataflow/'
        #     for i in os.listdir(path):
        #         p = os.path.join(path, i)
        #         if os.path.isfile(p) and '__tmp__' in i:
        #             table_name = i.split('__tmp__')[1]
        #             print(f'Mock insert of {table_name}')
        #             with open(p, 'r') as file1:
        #                 lines = list(map(lambda x: json.loads(x), file1.readlines()))
        #                 client = bigquery._bq.client
        #                 table = client.get_table(table_name)
        #                 job_config = bq.LoadJobConfig()
        #                 job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
        #                 job_config.schema = table.schema
        #                 job = client.load_table_from_json(lines, table, job_config=job_config)
        #                 job.result()
        #             os.remove(p)
        #             print(f'Done mock insert of {table_name}')

    return _r
