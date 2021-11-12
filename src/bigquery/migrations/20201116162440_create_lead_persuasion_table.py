from google.cloud import bigquery
from migrations.migration import BigQueryMigration
from google.api_core.exceptions import NotFound

dataset_name = 'wrench'
table_name = 'persuasion'


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.get_dataset(dataset_name)
    dataset = migration.create_dataset(dataset_name) if dataset is None else dataset

    schema = [
        bigquery.SchemaField('entity_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('persuasion', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('ingestion_timestamp', 'TIMESTAMP', mode='REQUIRED')
    ]
    migration.create_table(name=table_name,
                           project=migration.client.project, schema=schema, dataset=dataset,
                           partition={'type': 'time'},
                           clustering_fields=['entity_id:STRING'])
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
