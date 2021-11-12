import pytest
from load_vibe_to_lake import Runner, RuntimeOptions
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory
from libs.shared.test import skipif_prd
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider


@skipif_prd
@pytest.mark.skip(reason='Possibly seeding lake table instead of vibe table. 400 Table name "tree_users" missing dataset')
def test_end_to_end(env, bigquery):
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
        '--table',
        'tree_users',
        '--dest',
        'lake.tree_users'])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)
    rs = bigquery.query('select * from lake.tree_users')
    assert len(rs) == 3
