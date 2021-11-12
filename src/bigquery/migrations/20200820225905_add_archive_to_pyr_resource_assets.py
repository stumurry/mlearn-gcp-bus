from google.cloud import bigquery
from migrations.migration import BigQueryMigration
import logging

dataset_name = 'lake'
table_name = 'pyr_resource_assets'


def up(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)
    table = migration.client.get_table(dataset.table(table_name))
    orig_schema = table.schema
    new_schema = orig_schema.copy()

    if table.schema[-1].name == 'archive':
        logging.warning(f'created already added to {table_name} in {dataset_name} dataset!')
        return dataset

    new_schema.append(bigquery.SchemaField('archive', 'INTEGER'))
    table.schema = new_schema

    migration.client.update_table(table, ['schema'])
    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)
    return dataset
