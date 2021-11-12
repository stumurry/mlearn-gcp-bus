import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import RuntimeValueProvider
from libs import CloudStorage
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from transforms.io.filelist_iterator import IterateFilePathsFn
from transforms.io.filelist_iterator import FileListIteratorTransform
import dill
from unittest.mock import patch
from unittest import mock
from unittest.mock import Mock
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from apache_beam import pvalue


def test_IterateFilePathsFn_filters_and_sorts_with_static_params():
    def _sort_key(f):
        delimeter = '*'
        ts = f[f.rfind(delimeter) + 1:]
        return int(ts) if ts.isdigit() else f

    bucket_name = 'wrench-imports'
    _sort_key = bytes.hex(dill.dumps(_sort_key))

    mock_bucket = mock.create_autospec(Bucket)
    mock_bucket.name = bucket_name

    mock_blob = mock.create_autospec(Blob)
    mock_blob.name = 'wrench_test.csv'
    mock_blob.bucket = mock_bucket

    with patch.object(CloudStorage, 'list_blobs', return_value=[mock_blob]):
        expected_output = [
            'gs://wrench-imports/wrench_test.csv'
        ]

        with TestPipeline() as p:
            pcoll = (p | beam.Create([1])
                       | beam.ParDo(IterateFilePathsFn(
                            env='dev',
                            bucket=bucket_name,
                            sort_key=_sort_key,
                            files_startwith='',
                            file_name='.csv'
                        )))
            assert_that(pcoll, equal_to(expected_output))


def test_IterateFilePathsFn_filters_and_sorts_with_runtime_params():
    def _sort_key(f):
        delimeter = '-'
        ts = f[f.rfind(delimeter) + 1:]
        return int(ts) if ts.isdigit() else f

    bucket_name = 'cdc-imports'
    _sort_key = bytes.hex(dill.dumps(_sort_key))

    runtime_env = RuntimeValueProvider(
        option_name='env', value_type=str, default_value='dev'
    )
    runtime_bucket = RuntimeValueProvider(
        option_name='bucket', value_type=str, default_value=bucket_name
    )
    runtime_startswith = RuntimeValueProvider(
        option_name='files_startwith',
        value_type=str,
        default_value='vibe-tree-user-statuses-final'
    )
    runtime_sort_key = RuntimeValueProvider(
        option_name='sort_key', value_type=str, default_value=_sort_key
    )
    runtime_file_name = RuntimeValueProvider(
        option_name='file_name', value_type=str, default_value=''
    )

    mock_bucket = mock.create_autospec(Bucket)
    mock_bucket.name = bucket_name
    blob1 = mock.create_autospec(Blob)
    blob1.name = 'vibe-tree-user-statuses-final-2'
    blob1.bucket = mock_bucket

    blob2 = mock.create_autospec(Blob)
    blob2.name = 'vibe-tree-user-statuses-final-1'
    blob2.bucket = mock_bucket

    blob3 = mock.create_autospec(Blob)
    blob3.name = 'a-file-that-should-be-filtered-out-1'
    blob3.bucket = mock_bucket

    with patch.object(CloudStorage, 'list_blobs', return_value=[blob1, blob2]):
        expected_output = [
            'gs://cdc-imports/vibe-tree-user-statuses-final-1',
            'gs://cdc-imports/vibe-tree-user-statuses-final-2'
        ]

        with TestPipeline() as p:
            pcoll = (p | beam.Create([1])
                       | beam.ParDo(IterateFilePathsFn(
                            env=runtime_env,
                            bucket=runtime_bucket,
                            sort_key=runtime_sort_key,
                            files_startwith=runtime_startswith,
                            file_name=runtime_file_name
                        )))
            assert_that(pcoll, equal_to(expected_output))


def test_FileListIteratorTransform():
    env = 'mock_env'
    sort_key = 'sort_key'
    bucket = 'mock_bucket'
    files_startwith = 'files_startwith'
    file_name = 'files_startwith'
    sort_key = 'files_startwith'

    transform = FileListIteratorTransform(env=env, sort_key=sort_key, bucket=bucket,
                                          files_startwith=files_startwith, file_name=file_name)
    assert transform._env == env
    assert transform._bucket == bucket
    assert transform._files_startwith == files_startwith
    assert transform._file_name == file_name
    assert transform._sort_key == sort_key

    with TestPipeline() as p:
        def wraps(env, bucket, sort_key, files_startwith, file_name):
            fn = IterateFilePathsFn(env=env, bucket=bucket,
                                    sort_key=sort_key, files_startwith=files_startwith,
                                    file_name=file_name)
            fn.process = Mock(return_value='')
            return fn
        with patch('transforms.io.filelist_iterator.IterateFilePathsFn', wraps=wraps):
            begin = pvalue.PBegin(p)
            transform.expand(begin)
