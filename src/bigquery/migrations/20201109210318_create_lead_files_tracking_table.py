from google.cloud import bigquery
from migrations.migration import BigQueryMigration
from google.api_core.exceptions import NotFound

dataset_name = 'wrench'
table_name = 'data_source_status'


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.get_dataset(dataset_name)
    dataset = migration.create_dataset(dataset_name) if dataset is None else dataset

    schema = [
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('file', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("end_date", "DATETIME"),

    ]
    migration.create_table(name=table_name,
                           project=migration.client.project, schema=schema, dataset=dataset,
                           partition={'type': 'time'},
                           clustering_fields=['table:STRING', 'status:STRING'])
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
