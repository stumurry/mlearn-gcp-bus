from __future__ import print_function

import logging
import apache_beam as beam
import sys
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, SetupOptions
from transforms.datetime import StringifyDatetimes
from transforms.io.gcp import bigquery as bq


class EnrichRecord(beam.DoFn):
    def __init__(self, client):
        self._client = client

    def process(self, element):
        enriched = {**element, **{'icentris_client': self._client.get(),
                                  'leo_eid': 'z/0',
                                  'ingestion_timestamp': str(datetime.utcnow())}}
        yield enriched


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--client', help='iCentris client key ex. monat, worldventures, avon')
        parser.add_value_provider_argument('--table', help='Table to load from')
        parser.add_value_provider_argument('--dest', help='Table to write to. Ex: project_id:lake.pyr')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        """
        beam.io.BigQuerySource does not support ValueProvider (in this case, the 'query' arg). Until that happens we're
        forced to implement a customer PTransform that can execute a query that was passed in as an argument.
        10/7/2019 - jc
        """
        with p:
            (p
                | 'Read BigQuery Data' >> bq.ReadAsJson(
                    env=options.env,
                    table=options.table,
                    page_size=options.page_size)
                | 'Add Values for icentris_client, leo_id, ingestion_timestamp' >> beam.ParDo(
                    EnrichRecord(client=options.client))
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                | 'Write to staging' >> beam.io.WriteToBigQuery(
                                        options.dest,
                                        insert_retry_strategy='RETRY_NEVER',
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
             )

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.info('>> Running BigQuery to Cloud Storage pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_vibe_to_lake - Starting DataFlow Pipeline Runner')
    Runner.run()
