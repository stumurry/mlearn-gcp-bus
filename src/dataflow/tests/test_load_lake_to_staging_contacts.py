import pytest
from libs.shared.test import skipif_prd
from libs.shared.utils import parse_template
from load_lake_to_staging_contacts import Runner, RuntimeOptions
from apache_beam.testing.test_pipeline import TestPipeline


def test_end_to_end(env, sql_templates_path, bigquery):

    seeds = [
        ('lake', [
            ('contacts', [
                {
                    'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
                    'id':  1,
                    'icentris_client': 'bluesun',
                    'first_name': 'stu',
                    'last_name': 'test',
                    'email': 'stu@icentris.com'
                },
                {
                    'ingestion_timestamp': '2019-01-02 00:00:00.0 UTC',
                    'id':  1,
                    'icentris_client': 'bluesun',
                    'first_name': 'jon',
                    'last_name': 'test',
                    'email': 'jon@icentris.com'
                }
            ]),
        ]),
        ('staging', [
            ('contacts', [])
        ])]

    checkpoint = {
        'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
        'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
    }

    sql = parse_template(
        f'{sql_templates_path}/lake_to_staging.contacts.sql',
        **checkpoint)

    bigquery.truncate(seeds)
    bigquery.seed(seeds)

    # RuntimeValueProvider.set_runtime_options(None)
    print('Running pipeline.')
    options = RuntimeOptions([
        '--env',
        env['env'],
        '--query',
        sql])
    pipeline = TestPipeline(options=options)
    Runner._run(pipeline, options)

    rs = bigquery.query('select * from staging.contacts')

    assert len(rs) == 1


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_ingestion_timestamp_eid_windows_multi_users(run):
    # TODO: Pass checkpoint dynamically
    # checkpoint = {
    #     'first_ingestion_timestamp': '1970-01-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
    #     "first_eid": 'z/1970/01/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/01/01/00/00/0000000000000-0000000'
    # }
    assert len(run) == 4
    for r in run:
        continue
        if r['icentris_client'] == 'bluesun':
            if r['contact_id'] == 1:
                assert r['first_name'] == 'Jon'
                assert r['categories'][0]['category'] == 'bluesun One'
            elif r['contact_id'] == 2:
                assert r['first_name'] == 'Jane'
            else:
                assert False
        elif r['icentris_client'] == 'worldventures':
            if r['contact_id'] == 1:
                assert r['first_name'] == 'Matthew'
            elif r['contact_id'] == 2:
                assert r['first_name'] == 'Titus'
            else:
                assert False
        else:
            assert False


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_ingestion_timestamp_eid_windows_single_user(run):
    # TODO: Pass checkpoint dynamically
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-02-01 00:00:00.0 UTC',
    #     "first_eid": 'z/2019/01/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/02/01/00/00/0000000000000-0000000'
    # }

    assert len(run) == 4
    # TODO - Come back later and get these assertions working
    # assert run[0]['icentris_client'] == 'worldventures'
    # assert run[0]['contact_id'] == 2
    # assert run[0]['first_name'] == 'TitusX'


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_replay_event(run):
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-03-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-03-01 12:00:00.0 UTC',
    #     "first_eid": 'z/2019/03/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/03/01/12/00/0000000000000-0000000'
    # }

    assert len(run) == 1
    assert run[0]['icentris_client'] == 'monat'
    assert run[0]['id'] == 1
    assert run[0]['first_name'] == 'Jon-Replay'


@skipif_prd
@pytest.mark.skip(reason='Figure out how to dynamically update the checkpoint')
def test_joined_table_event(run):
    # checkpoint = {
    #     'first_ingestion_timestamp': '2019-05-01 00:00:00.0 UTC',
    #     'last_ingestion_timestamp': '2019-05-01 12:00:00.0 UTC',
    #     "first_eid": 'z/2019/05/01/00/00/0000000000000-0000000',
    #     "last_eid": 'z/2019/05/01/12/00/0000000000000-0000000'
    # }

    assert len(run) == 1
    assert run[0]['icentris_client'] == 'bluesun'
    assert run[0]['id'] == 1
    assert any(e['email'] == 'jonathan.doe@test.com' for e in run[0]['emails'])
