from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'lake'
table_name = 'pyr_resources'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name.format(client.env))

    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    new_schema.append(bigquery.SchemaField('extras', 'STRING', 'NULLABLE'))
    table.schema = new_schema

    migration.client.update_table(table, ['schema'])
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name.format(client.env))
    return dataset
