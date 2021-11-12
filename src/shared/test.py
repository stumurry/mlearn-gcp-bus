import logging
import os
import pytest
from google.cloud import bigquery
from .gcloud import GCLOUD as gcloud
from .bigquery import BigQuery
from .storage import CloudStorage
from unittest import mock
import google.cloud.storage._http
from google.cloud.exceptions import NotFound

log = logging.getLogger()


class BigQueryFixture():
    me = None

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = BigQueryFixture(env)
        return cls.me

    def __init__(self, env):
        project = gcloud.project(env)
        self._bq = BigQuery.instance(env)
        self._datasets = {'bluesun': bigquery.Dataset('{}.{}'.format(project, 'pyr_bluesun_{}'.format(env))),
                          'pii': bigquery.Dataset('{}.{}'.format(project, 'pii')),
                          'lake': bigquery.Dataset('{}.{}'.format(project, 'lake')),
                          'system': bigquery.Dataset('{}.{}'.format(project, 'system')),
                          'staging': bigquery.Dataset('{}.{}'.format(project, 'staging')),
                          'wrench': bigquery.Dataset('{}.{}'.format(project, 'wrench')),
                          'warehouse': bigquery.Dataset('{}.{}'.format(project, 'warehouse'))}
        self._tables = []

    def query(self, sql):
        return self._bq.query(sql)

    def seed(self, seeds):
        for dataset, tables in seeds:
            ds = self._datasets[dataset]
            print('Seeding...')
            for tbl, rows in tables:

                if len(rows) > 0:
                    print(rows)
                    client = self._bq.client
                    table_name = f'{ds.dataset_id}.{tbl}'
                    print('table:', table_name)
                    table = client.get_table(table_name)
                    job_config = bigquery.LoadJobConfig()
                    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                    job_config.schema = table.schema
                    job = client.load_table_from_json(rows, table, job_config=job_config)
                    job.result()
                    print('Done seeding:', table_name)

    def truncate(self, seeds):
        queues = [
            # self._bq.create_queue(self._bq.client.delete_table),
            # self._bq.create_queue(self._bq.client.copy_table),
            # self._bq.create_queue(self._bq.client.delete_table),
            # self._bq.create_queue(self._bq.client.copy_table),
            self._bq.create_queue(self._bq._query)
        ]
        for dataset, tables in seeds:
            ds = self._datasets[dataset]

            for tbl, rows in tables:
                # orig_tbl_id = f'{self._bq.client.project}.{ds.dataset_id}.{tbl}'
                # cp_tbl_id = orig_tbl_id + '_cp'
                # queues[0].append(cp_tbl_id, not_found_ok=True)
                # queues[1].append(orig_tbl_id, cp_tbl_id)
                # queues[2].append(orig_tbl_id, not_found_ok=True)
                # queues[3].append(cp_tbl_id, orig_tbl_id)
                queues[0].append(f'DELETE FROM `{ds.dataset_id}.{tbl}` WHERE 1=1')

        try:
            for i, q in enumerate(queues):
                q.run()
        except Exception:
            # print(queues[0].errors)
            logging.getLogger().error(queues[0].errors)
            raise


class CloudStorageFixture():
    me = None

    @classmethod
    def instance(cls, env):
        if cls.me is None:
            cls.me = CloudStorageFixture(env)
        return cls.me

    def __init__(self, env):
        project = gcloud.project(env)
        self.client = CloudStorage(project)

    @classmethod
    def mock_connection(cls, *responses):
        mock_conn = mock.create_autospec(google.cloud.storage._http.Connection)
        mock_conn.user_agent = 'testing 1.2.3'
        mock_conn.api_request.side_effect = list(responses) + [NotFound('miss')]
        return mock_conn


skipif_prd = pytest.mark.skipif(os.environ.get('ENV', 'prd') == 'prd', reason='Skipped for production environment')
skipif_dev = pytest.mark.skipif(os.environ.get('ENV', 'dev') == 'dev', reason='Skipped for ci/cd environment')
skipif_local = pytest.mark.skipif(os.environ.get('ENV', 'dev') != 'dev' and os.environ.get(
    'ENV', 'prd') != 'prd', reason='Skipped for local environment')
