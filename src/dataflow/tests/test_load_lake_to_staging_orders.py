from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import LakeTreeUserFactory, LakeTreeOrderFactory
from load_lake_to_staging_orders import Runner, RuntimeOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
import pytest


@skipif_prd
@pytest.mark.skip(reason='Skipping for now.')
def test_end_to_end(env, sql_templates_path, bigquery):
    start = '2020-06-01 12:00:00 UTC'
    end = '2020-06-01 12:00:01 UTC'

    FactoryRegistry.create_multiple(LakeTreeUserFactory, 2, [
        {'icentris_client': 'bluesun'},
        {'icentris_client': 'worldventures'}
    ])
    FactoryRegistry.create_multiple(LakeTreeOrderFactory, 2, [
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][0]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][0]['icentris_client'],
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': start},
        {'tree_user_id': FactoryRegistry.registry['LakeTreeUserFactory'][1]['id'],
         'icentris_client': FactoryRegistry.registry['LakeTreeUserFactory'][1]['icentris_client'],
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': start},
    ])

    seeds = [
        ('lake', [
            ('tree_users', FactoryRegistry.registry['LakeTreeUserFactory']),
            ('tree_user_types', FactoryRegistry.registry['LakeTreeUserTypeFactory']),
            ('tree_user_statuses', FactoryRegistry.registry['LakeTreeUserStatusFactory']),
            ('tree_orders', FactoryRegistry.registry['LakeTreeOrderFactory']),
            ('tree_order_types', FactoryRegistry.registry['LakeTreeOrderTypeFactory']),
            ('tree_order_statuses', FactoryRegistry.registry['LakeTreeOrderStatusFactory'])
        ]),
        ('staging', [
            ('orders', [])
        ])
    ]

    checkpoint = {
        'first_ingestion_timestamp': start,
        'last_ingestion_timestamp': end
    }

    sql = parse_template(
        f'{sql_templates_path}/lake_to_staging.orders.sql', **checkpoint)

    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    RuntimeValueProvider.set_runtime_options(None)
    print('Running pipeline.')
    options = RuntimeOptions([
        '--env',
        env['env'],
        '--query',
        sql])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)

    rs = bigquery.query('select * from staging.orders')
    assert len(rs) == 2
