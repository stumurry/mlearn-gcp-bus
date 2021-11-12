from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from migrations.migration import BigQueryMigration

dataset_name = 'lake'
table_name = 'wrench_metrics'


def up(client):
    migration = BigQueryMigration(client)

    dataset = migration.dataset(dataset_name)  # use me if you are NOT creating a new dataset. -- ndg 2/5/20

    clusters = migration.default_clustering_fields

    del clusters[0]

    clusters.insert(0, 'client_wrench_id:STRING')

    schema = [
        bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("client_wrench_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField('entity_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField("contact_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("owner_tree_user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("processing_datetime", "DATETIME"),
        bigquery.SchemaField('metrics', 'RECORD', mode='REPEATED', fields=[
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('value', 'STRING')
        ])

    ]

    migration.create_table(name=table_name,
                           project=migration.client.project, schema=schema, dataset=dataset,
                           partition={'type': 'time'},
                           clustering_fields=clusters)

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
