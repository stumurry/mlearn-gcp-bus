from libs.shared.utils import parse_template
from load_staging_to_warehouse_distributor_orders import Runner, RuntimeOptions
from libs.shared.test_factories import FactoryRegistry
from libs.shared.test_factories import StagingUserFactory, StagingOrderFactory
from libs.shared.test import skipif_prd
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
import pytest


@skipif_prd
@pytest.mark.skip(reason='Skipping for now.')
def test_end_to_end(env, sql_templates_path, bigquery):
    start = '1970-01-01 00:00:00 UTC'
    end = '1970-01-01 00:00:00 UTC'

    client = 'bluesun'

    FactoryRegistry.create_multiple(StagingUserFactory, 1, [
        {'icentris_client': client,
         'tree_user_id': 1,
         'client_status': 'Active',
         'type_': 'Distributor',
         'leo_eid': 'z/1970/01/02/00/00/0000000000001-0000001',
         'ingestion_timestamp': end,
         'lifetime_rank': 'GOLD'},
    ])

    FactoryRegistry.create_multiple(StagingOrderFactory, 2, [
        {'icentris_client': client,
         'leo_eid': 'z/1970/01/02/00/00/0000000000001-0000001',
         'commission_user_id': 1,
         'tree_user_id': 1,
         'type_': 'Autoship',
         'order_id': 1,
         'ingestion_timestamp': end},
        {'icentris_client': client,
         'commission_user_id': 1,
         'tree_user_id': 1,
         'order_id': 2,
         'type_': 'Autoship',
         'leo_eid': 'z/1970/01/01/00/00/0000000000001-0000001',
         'ingestion_timestamp': end},
    ])

    seeds = [
        ('staging', [
            ('users', FactoryRegistry.registry['StagingUserFactory']),
            ('orders', FactoryRegistry.registry['StagingOrderFactory'])
        ]), ('warehouse', [
            ('distributor_orders', [])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': start,
        'last_ingestion_timestamp': end
    }

    sql = parse_template(
        f'{sql_templates_path}/staging_to_warehouse.distributor_orders.sql', **checkpoint)
    print('running pipeline')

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

    rs = bigquery.query('select * from warehouse.distributor_orders')
    assert len(rs) == 1
