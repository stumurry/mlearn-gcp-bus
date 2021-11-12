from google.cloud import bigquery
from migrations.migration import BigQueryMigration

dataset_name = 'staging'
table_name = 'commissions'


schema = [
    bigquery.SchemaField('client_partition_id', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('icentris_client', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('tree_user_id', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('client_user_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField(
        "commissisions",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("currency_code", "STRING"),
            bigquery.SchemaField('earnings', 'INTEGER'),
            bigquery.SchemaField('previous_balance', 'INTEGER'),
            bigquery.SchemaField('balance_forward', 'INTEGER'),
            bigquery.SchemaField('fee', 'INTEGER'),
            bigquery.SchemaField('total', 'INTEGER'),
            bigquery.SchemaField('checksum', 'STRING'),
                ]
    ),
    bigquery.SchemaField(
        "details",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('source_amount', 'INTEGER'),
            bigquery.SchemaField('percentage', 'INTEGER'),
            bigquery.SchemaField('commission_amount', 'INTEGER'),
            bigquery.SchemaField('level', 'INTEGER'),
            bigquery.SchemaField('paid_level', 'INTEGER')
        ]
    ),
    bigquery.SchemaField(
        "bonuses",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('amount', 'INTEGER'),
            bigquery.SchemaField('description', 'STRING')
        ]
    ),
    bigquery.SchemaField(
        "runs",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('period', 'INTEGER'),
            bigquery.SchemaField('period_type', 'STRING'),
            bigquery.SchemaField('description', 'STRING')
        ]
    ),
    bigquery.SchemaField('created', 'DATETIME'),
    bigquery.SchemaField('modified', 'DATETIME')
]


def up(client):
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
        clustering_fields=migration.default_clustering_fields
    )

    return dataset


def down(client):
    migration = BigQueryMigration(client)
    dataset = migration.dataset(dataset_name)
    table = migration.client.get_table(dataset.table(table_name))
    migration.delete_table(table)
