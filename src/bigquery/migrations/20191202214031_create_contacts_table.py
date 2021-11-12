from google.cloud import bigquery
from migrations.migration import BigQueryMigration


dataset_name = 'staging'
table_name = 'contacts'


def up(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)

    contacts = [
        bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('first_name', 'STRING'),
        bigquery.SchemaField('last_name', 'STRING'),
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
        bigquery.SchemaField('country', 'STRING')
    ]

    client.create_table(
        name=table_name,
        project=client.client.project,
        schema=contacts,
        dataset=dataset,
        partition={
            'type': 'range',
            'field': 'client_partition_id',
            'start': 1,
            'end': 100,
            'interval': 1
        },
        clustering_fields=['ingestion_timestamp:TIMESTAMP'])
    return dataset


def down(client):
    client = BigQueryMigration(client)
    dataset = client.dataset(dataset_name)
    table = client.client.get_table(dataset.table(table_name))
    client.delete_table(table)
