from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory, LakeUserFactory
from load_lake_to_staging_users import Runner, RuntimeOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
import pytest


@skipif_prd
@pytest.mark.skip(reason='Skipping for now.')
def test_end_to_end(env, sql_templates_path, bigquery):
    start = '2020-06-01 12:00:00 UTC'
    end = '2020-06-01 12:00:01 UTC'

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 2, [
        {'icentris_client': 'bluesun',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': start},
        {'icentris_client': 'worldventures',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': end}
    ])
    FactoryRegistry.create_multiple(LakeUserFactory, 2, [
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][0]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][0]['icentris_client']},
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][1]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][1]['icentris_client']},
    ])

    rs = bigquery.query('select * from staging.users')
    print('\nrs count', len(rs))
    print('rs', rs)

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory']),
            ('users', FactoryRegistry.registry['LakeUserFactory']),
            ('tree_user_types', FactoryRegistry.registry['LakeTreeUserTypeFactory']),
            ('pyr_rank_definitions', FactoryRegistry.registry['LakePyrRankDefinitionFactory']),
            ('tree_user_statuses', FactoryRegistry.registry['LakeTreeUserStatusFactory'])
        ]),
        ('staging', [
            ('users', [])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': start,
        'last_ingestion_timestamp': end
    }

    sql = parse_template(
        f'{sql_templates_path}/lake_to_staging.users.sql', **checkpoint)

    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    RuntimeValueProvider.set_runtime_options(None)
    print('Running pipeline.')
    options = RuntimeOptions([
        '--env',
        env['env'],
        '--query',
        sql,
        '--project',
        'icentris-ml-local-smurry'])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)

    rs = bigquery.query('select * from staging.users')
    assert len(rs) == 2
