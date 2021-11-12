import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, StandardOptions, SetupOptions
import json
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.filesystem import CompressionTypes


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--users', help='storage location of users')
        parser.add_value_provider_argument('--streams', help='storage location of streams')
        parser.add_value_provider_argument('--tracks', help='storage location of tracks')
        parser.add_value_provider_argument('--output', help='Table to write to. Ex: project_id:lake.pyr')


def run():
    log = logging.getLogger()
    log.warning('>> Running UMG Exam')

    options = RuntimeOptions()

    with beam.Pipeline(options=options) as p:
        users = (p | 'Read Users' >> ReadFromText(options.users)
                   | 'Parse Users' >> beam.Map(json.loads))
        tracks = (p | 'Read Tracks' >> ReadFromText(options.tracks)
                    | 'Parse Tracks' >> beam.Map(json.loads))
        (p | 'Read Streams' >> ReadFromText(options.streams)
           | 'Parse Streams' >> beam.Map(json.loads)
           | 'Overlay User and Track' >> beam.Map(
               lambda s, t, u: {k: v for i in [s, list(filter(lambda x: x['track_id'] == s['track_id'], t))[0], list(filter(lambda y: y['user_id'] == s['user_id'], u))[0]] for k, v in i.items()},  # noqa: E501
               beam.pvalue.AsList(tracks),
               beam.pvalue.AsList(users))
           | 'Write To Cloud Storage' >> WriteToText(options.output, num_shards=1, compression_type=CompressionTypes.GZIP, file_name_suffix='.gz', shard_name_template=''))  # noqa: E501


# python -m umg_exam --streams gs://stu-scratch/streams.gz \
# --tracks gs://stu-scratch/tracks.gz \
# --users gs://stu-scratch/users.gz \
# --output gs://stu-scratch/test
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> umg_exam_test - Starting DataFlow Pipeline Runner')
    run()