import pytest
import logging
from load_sql_to_bq import Runner, RuntimeOptions
from libs import GCLOUD as gcloud
from libs.shared.test import skipif_prd
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


@skipif_prd
@pytest.mark.skip(reason='Reading records from CloudSql needs to be mocked')
def test_end_to_end(env, bigquery):
    project_id = gcloud.project(env['env'])

    end = '2020-06-01 12:00:01 UTC'

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 3, [
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': end},
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': end},
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': end}
    ])

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory'])
        ])]

    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    RuntimeValueProvider.set_runtime_options(None)
    print('Running pipeline.')
    options = RuntimeOptions([
        '--client',
        env['client'],
        '--env',
        env['env'],
        '--bq_table',
        '{}:pyr_bluesun_local.tree_users'.format(project_id),
        '--table',
        'tree_users',
        '--key_field',
        'id'])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)

    rs = bigquery.query('select * from lake.tree_users')
    return rs
