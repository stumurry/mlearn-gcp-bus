from google.cloud import bigquery
from migrations.migration import BigQueryMigration
from google.api_core.exceptions import NotFound

dataset_name = 'staging'
table_name = 'zleads'


def up(client):
    schema = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('lead_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('purchased', 'BOOLEAN', mode='REQUIRED'),
        bigquery.SchemaField('first_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('last_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('email', 'STRING'),
        bigquery.SchemaField('email2', 'STRING'),
        bigquery.SchemaField('email3', 'STRING'),
        bigquery.SchemaField('phone', 'STRING'),
        bigquery.SchemaField('phone2', 'STRING'),
        bigquery.SchemaField('phone3', 'STRING'),
        bigquery.SchemaField('twitter', 'STRING'),
        bigquery.SchemaField('city', 'STRING'),
        bigquery.SchemaField('state', 'STRING'),
        bigquery.SchemaField('zip', 'STRING'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('gender', 'STRING'),
        bigquery.SchemaField('age', 'INTEGER'),
        bigquery.SchemaField('created', 'DATETIME'),
        bigquery.SchemaField('modified', 'DATETIME')
    ]

    migration = BigQueryMigration(client)

    dataset = migration.get_dataset(dataset_name)
    dataset = migration.create_dataset(dataset_name) if dataset is None else dataset
    migration.create_table(
        name=table_name,
        project=migration.client.project, schema=schema, dataset=dataset,
        partition={
            'type': 'range',
            'field': 'client_partition_id',
            'start': 1,
            'end': 100,
            'interval': 1
        },
        clustering_fields=['purchased:BOOLEAN', 'ingestion_timestamp:TIMESTAMP']
    )

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)
    table = None

    try:
        table = migration.client.get_table(dataset.table(table_name))
    except NotFound:
        pass
    except Exception as e:
        raise e

    if table:
        migration.delete_table(table)
