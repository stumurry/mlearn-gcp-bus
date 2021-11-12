
import wrench_tasks


DAG_ID = wrench_tasks.DAG_ID


def test_dag(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args == wrench_tasks.default_args
    if dag.catchup:
        assert False


def test_bq_update_lead_ids_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.get_task('bq_update_lead_ids_task') is not None


def test_bq_update_contact_enitity_lookup_task(load_dag):
    dag_bag = load_dag(DAG_ID)
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.get_task('bq_update_contact_ids_task') is not None


seeds = [
    ('staging', [
        ('zleads', [
            {
                'lead_id': 1, 'first_name': 'Patrick', 'last_name': 'Doe',
                'email': 'patrick@icentris.com', 'phone': '5', 'purchased': False,
                'twitter': 'hashtag_patrick',
                'icentris_client': 'bluesun', 'client_partition_id': 4,
                'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
                'created': '1970-01-01'
            },
            {
                'lead_id': 1, 'first_name': 'Patrick', 'last_name': 'Doe',
                'email': 'patrick@icentris.com', 'phone': '5', 'purchased': False,
                'twitter': 'hashtag_patrick',
                'icentris_client': 'plexus', 'client_partition_id': 6,
                'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
                'created': '1970-01-01'
            }
        ]),
        ('contacts', [
            {
                'id': 1,
                'first_name': 'patrick',
                'last_name': 'tester',
                'email': 'patrick@icentris.com',
                'ingestion_timestamp': '2019-01-01 00:00:00.0 UTC',
                'icentris_client': 'bluesun', 'client_partition_id': 4,
            },
        ])
    ]),
    ('wrench', [
        ('lead_entities_lk', []),
        ('contact_entities_lk', []),
        ('entities', [
            {
                'icentris_client': 'bluesun',
                'entity_id': 'ebb878bf-8a8f-41ec-9508-246f461eb8ef',
                'file': '20210115193834-staging.contacts-bq-to-wrench-123456789.csv',
                'email': 'patrick@icentris.com',
                'phone': '5',
                'twitter': 'hashtag_patrick',
                'ingestion_timestamp': '2021-01-15 20:45:28.226824 UTC'
            },
            {
                'icentris_client': 'bluesun',
                'entity_id': '12345s-8a8f-41ec-9508-246f461eb8ef',
                'file': '20210115193834-staging.zleads-bq-to-wrench-123456789.csv',
                'email': 'patrick@icentris.com',
                'phone': '5',
                'twitter': 'hashtag_patrick',
                'ingestion_timestamp': '2021-01-15 20:45:28.226824 UTC'
            },
            {
                'icentris_client': 'plexus',
                'entity_id': 'asdf-asdf-asdf-asdf',
                'file': '20210115193834-staging.zleads-bq-to-wrench-123456789.csv',
                'email': 'patrick@icentris.com',
                'phone': '5',
                'twitter': 'hashtag_patrick',
                'ingestion_timestamp': '2021-12-25 20:45:28.226824 UTC'
            }
        ]),
        ('data_source_status', [
            {'icentris_client': 'bluesun', 'file': '20210115193834-staging.contacts-bq-to-wrench-123456789.csv',
                'status': 'processed',
                'table': 'staging.contacts', 'start_date': '2019-01-01 00:00:00'},
            {'icentris_client': 'bluesun', 'file': '20210115193834-staging.zleads-bq-to-wrench-123456789.csv',
                'status': 'processed',
                'table': 'staging.zleads', 'start_date': '2019-01-01 00:00:00'},
            {'icentris_client': 'plexus', 'file': '20211225193834-staging.zleads-bq-to-wrench-987654321.csv',
                'status': 'processed',
                'table': 'staging.zleads', 'start_date': '2019-01-01 00:00:00'}
        ])
    ])
]


def test_bq_update_lead_ids(seed, load_dag, bigquery_helper):
    seed(seeds)

    wrench_tasks.bq_update_lead_ids()

    resp = bigquery_helper.query(sql='select * from wrench.lead_entities_lk')
    assert len(resp) == 1

    wrench_tasks.bq_update_contact_ids()

    resp = bigquery_helper.query(sql='select * from wrench.contact_entities_lk')
    assert len(resp) == 1
