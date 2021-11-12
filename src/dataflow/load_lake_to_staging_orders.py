from __future__ import print_function

import logging
import apache_beam as beam
import sys
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, SetupOptions
from transforms.io.gcp import bigquery as bq
from transforms.worldventures import WorldVenturesStagingOrdersTransform
from transforms.datetime import StringifyDatetimes
from transforms.decimal import StringifyDecimal


class Bluesun(beam.DoFn):
    def process(self, payload):
        icentris_client = payload['icentris_client'].lower()
        if icentris_client == 'bluesun':
            payload['type'] = 'Customer'
            payload['status'] = 'Active'
            payload['commission_user_id'] = payload['tree_user_id']
            yield payload


class PrewriteCleanup(beam.DoFn):
    def process(self, payload):
        del payload['sponsor_id']
        del payload['client_user_type']
        yield payload


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--dest',
                                           default='staging.orders',
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
                                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                                | 'Transform Decimals' >> beam.ParDo(StringifyDecimal()))

            wv_p = (big_query_data | 'Apply World Ventures Transform' >> WorldVenturesStagingOrdersTransform())

            bs_p = (big_query_data | 'Apply Bluesun Transform' >> beam.ParDo(Bluesun()))

            ((wv_p, bs_p) | 'Merge Client Collections for Writing to BigQuery' >> beam.Flatten()
                | 'Prewrite Cleanup' >> beam.ParDo(PrewriteCleanup())
                | 'Write To BigQuery' >> bq.WriteToBigQueryTransform(table=options.dest, env=options.env))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running BigQuery to Cloud Storage pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_lake_to_staging - Starting DataFlow Pipeline Runner')
    Runner().run()
