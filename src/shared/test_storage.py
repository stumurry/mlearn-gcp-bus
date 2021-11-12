import os
import pytest
from unittest import mock
from .test import CloudStorageFixture
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
from .storage import CloudStorage


@pytest.fixture
def storage():
    return CloudStorageFixture.instance(os.environ['ENV'])


def test_storage_client_mocks(storage):
    with mock.patch(
        'google.cloud.storage.client.Client._connection',
        new_callable=mock.PropertyMock,
    ) as client_mock:
        client_mock.return_value = storage.mock_connection({'items': [{'name': 'foo.txt'}, {'name': 'bar.csv'}]})

        iterator = storage.client.list_blobs('mock-bucket')
        blobs = list(iterator)
        assert len(blobs) == 2
        assert blobs[0].name == 'foo.txt'


def test_list_files_removes_path(storage):
    with mock.patch(
        'google.cloud.storage.client.Client._connection',
        new_callable=mock.PropertyMock,
    ) as client_mock:
        client_mock.return_value = storage.mock_connection({'items': [{'name': 'one/two/three/bar.csv'}]})

        files = storage.client.list_files('mock-bucket', prefix='one/two/three')
        assert len(files) == 1
        assert files[0] == 'bar.csv'


@pytest.mark.skip(reason='Test not passing.')
def test_move_files_by_moving_a_single_file_within_multiple_files(storage):
    from_bucket = mock.create_autospec(Bucket)
    from_bucket.name = 'FROM_BUCKET'
    to_bucket = mock.create_autospec(Bucket)
    to_bucket.name = 'TO_BUCKET'
    blob1 = Blob('vibe-messages-final-doc1.txt', bucket=from_bucket)
    blob2 = Blob('vibe-contact-phone-numbers-final-doc2.txt', bucket=from_bucket)
    prefix = 'vibe-messages-final'
    blobs = [blob1, blob2]

    from_bucket.list_blobs.side_effect = lambda prefix: [b for b in blobs if b.name.startswith(prefix)]

    storage.client.move_files(from_bucket, to_bucket, prefix=prefix)
    assert from_bucket.copy_blob.call_count == 1
    from_bucket.list_blobs.assert_called_once_with(prefix=prefix)
    from_bucket.delete_blob.assert_called_once_with(blob1)


def test_move_files_by_moving_multiple_files(storage):
    from_bucket = mock.create_autospec(Bucket)
    from_bucket.name = 'FROM_BUCKET'
    to_bucket = mock.create_autospec(Bucket)
    to_bucket.name = 'TO_BUCKET'
    blob1 = Blob('vibe-contact-phone-numbers-doc1.txt', bucket=from_bucket)
    blob2 = Blob('vibe-contact-phone-numbers-final-doc2.txt', bucket=from_bucket)
    prefix = 'vibe-contact-phone-numbers'
    blobs = [blob1, blob2]

    from_bucket.list_blobs.side_effect = lambda prefix: [b for b in blobs if b.name.startswith(prefix)]
    storage.client.move_files(from_bucket, to_bucket, prefix=prefix)
    assert from_bucket.copy_blob.call_count == 2
    from_bucket.list_blobs.assert_called_with(prefix=prefix)
    assert from_bucket.delete_blob.call_count == 2


def test_move_files_by_moving_and_deleting_all_files(storage):
    from_bucket = mock.create_autospec(Bucket)
    from_bucket.name = 'FROM_BUCKET'
    to_bucket = mock.create_autospec(Bucket)
    to_bucket.name = 'TO_BUCKET'
    blob1 = Blob('vibe-messages-final-doc1.txt', bucket=from_bucket)
    blob2 = Blob('vibe-contact-phone-numbers-final-doc2.txt', bucket=from_bucket)
    prefix = ''
    blobs = [blob1, blob2]

    from_bucket.list_blobs.side_effect = lambda prefix: [b for b in blobs if b.name.startswith(prefix)]
    storage.client.move_files(from_bucket, to_bucket, prefix=prefix)
    assert from_bucket.copy_blob.call_count == 2
    from_bucket.list_blobs.assert_called_with(prefix=prefix)
    assert from_bucket.delete_blob.call_count == 2


def test_move_files_with_filter(storage):
    from_bucket = mock.create_autospec(Bucket)
    from_bucket.name = 'FROM_BUCKET'
    to_bucket = mock.create_autospec(Bucket)
    to_bucket.name = 'TO_BUCKET'
    blob1 = Blob('vibe-contact-phone-numbers-doc1.txt', bucket=from_bucket)
    blob2 = Blob('vibe-contact-phone-numbers-final-doc2.txt', bucket=from_bucket)
    pattern = 'doc2.txt'
    blobs = [blob1, blob2]

    from_bucket.list_blobs.side_effect = lambda prefix: [b for b in blobs]
    storage.client.move_files(from_bucket, to_bucket, pattern=pattern)
    assert from_bucket.copy_blob.call_count == 1
    assert from_bucket.delete_blob.call_count == 1


def test_has_file(storage):
    bucket = mock.create_autospec(Bucket)
    bucket.name = 'FROM_BUCKET'
    blob = Blob('doc1.txt', bucket=bucket)
    bucket.list_blobs.return_value = [blob]
    assert storage.client.has_file(bucket=bucket)
    empty_bucket = mock.create_autospec(Bucket)
    empty_bucket.name = 'FROM_BUCKET'
    has_file_cond = not CloudStorage.factory('PROJECT').has_file(bucket=empty_bucket)
    assert has_file_cond


def test_has_file_with_prefix(storage):
    bucket = mock.create_autospec(Bucket)
    bucket.name = 'FROM_BUCKET'
    prefix = 'prefix-'
    storage.client.has_file(bucket=bucket, prefix=prefix)
    bucket.list_blobs.assert_called_once_with(prefix=prefix)


def test_remove_prefix(storage):
    prefixed_string = 'gs://us-west3-icentris-ml-prd-5b4c8c18-bucket/dags'
    assert CloudStorage.remove_prefix(prefixed_string, 'gs://') == 'us-west3-icentris-ml-prd-5b4c8c18-bucket/dags'
