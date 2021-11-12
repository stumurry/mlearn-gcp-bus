from __future__ import print_function

import logging
import apache_beam as beam
import sys
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, StandardOptions, SetupOptions

from transforms.io.gcp import bigquery as bq
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes
from transforms.decimal import StringifyDecimal
from transforms.worldventures import WorldVenturesWarehouseDistributedOrdersTransform


class Bluesun(beam.DoFn):
    def process(self, element):
        icentris_client = element['icentris_client'].lower()
        if icentris_client == 'bluesun':
            del element['client_status']
            del element['client_type']
            yield element


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--dest',
                                           default='warehouse.distributor_orders',
                                           help='Table to write to. Ex: project_id:lake.pyr')
        parser.add_value_provider_argument('--query', type=str, help='BigQuery query statement')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            big_query_data = (p | 'Read BigQuery Data' >> bq.ReadAsJson(
                env=options.env,
                query=options.query,
                page_size=options.page_size)
                | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                | 'Transform Decimals' >> beam.ParDo(StringifyDecimal()))

            wv_p = (big_query_data | 'Apply World Ventures Transform' >> WorldVenturesWarehouseDistributedOrdersTransform())

            bs_p = (big_query_data | 'Apply Bluesun Transform' >> beam.ParDo(Bluesun()))

            results = ((wv_p, bs_p) | 'Merge Client Collections for Writing to BigQuery' >> beam.Flatten()
                       | 'Write To BigQuery' >> bq.WriteToBigQueryTransform(table=options.dest, env=options.env))
            print(results)
            # (results['FailedRows'] | 'Print Failures' >> beam.ParDo(Log()))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running Staging To Warehouse pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        options.view_as(StandardOptions).streaming = False

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_staging_to_warehouse - Starting DataFlow Pipeline Runner')
    Runner.run()
