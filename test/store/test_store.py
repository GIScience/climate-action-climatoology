import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import minio.datatypes
import pytest

from climatoology.base.operator import Artifact, ArtifactModality
from climatoology.store.object_store import MinioStorage


@pytest.fixture()
def mocked_client():
    with patch('climatoology.store.object_store.Minio') as minio_client:
        minio_storage = MinioStorage(host='minio.test.org', port=9999, access_key='key', secret_key='secret', secure=True, bucket='test_bucket')
        yield {'minio_storage': minio_storage, 'minio_client': minio_client}


def test_minio_save(mocked_client):
    correlation_uuid = uuid.uuid4()
    store_uuid = mocked_client['minio_storage'].save(Artifact(correlation_uuid, ArtifactModality.MAP_LAYER, Path('/tmp/map.tiff')))

    mocked_client['minio_client'].assert_called_once_with(endpoint='minio.test.org:9999',
                                                          access_key='key',
                                                          secret_key='secret',
                                                          secure=True)
    mocked_client['minio_client']().fput_object.assert_called_once_with(bucket_name='test_bucket',
                                                                        file_path='/tmp/map.tiff',
                                                                        metadata={
                                                                            'Correlation-UUID': correlation_uuid,
                                                                            'Modality': 'MAP_LAYER',
                                                                            'Original-Filename': 'map.tiff'
                                                                        },
                                                                        object_name=f'{correlation_uuid}/{store_uuid}')


def test_minio_fetch_all(mocked_client):
    correlation_uuid = uuid.uuid4()
    store_uuid = uuid.uuid4()
    mocked_client['minio_client']().list_objects.return_value = iter([minio.datatypes.Object(bucket_name='test_bucket',
                                                                                             object_name=f'{correlation_uuid}/{store_uuid}',
                                                                                             metadata={
                                                                                                 'X-Amz-Meta-Original-Filename': 'test.tiff',
                                                                                                 'X-Amz-Meta-Modality': 'MAP_LAYER'
                                                                                             })])

    with TemporaryDirectory() as temp_dir:
        result = mocked_client['minio_storage'].fetch_all(correlation_uuid, Path(temp_dir))
        assert result == [Artifact(correlation_uuid, ArtifactModality.MAP_LAYER, Path(temp_dir) / 'test.tiff')]

        mocked_client['minio_client']().list_objects.assert_called_once_with(bucket_name='test_bucket', prefix=str(correlation_uuid), recursive=True, include_user_meta=True)
        mocked_client['minio_client']().fget_object.assert_called_once_with(bucket_name='test_bucket', object_name=f'{correlation_uuid}/{store_uuid}', file_path=Path(temp_dir) / 'test.tiff')


def test_minio_save_all(mocked_client):
    correlation_uuid = uuid.uuid4()
    mocked_client['minio_storage'].save_all([
        Artifact(correlation_uuid, ArtifactModality.MAP_LAYER, Path('/tmp/map.tiff')),
        Artifact(correlation_uuid, ArtifactModality.TEXT, Path('/tmp/text.txt')),
        Artifact(correlation_uuid, ArtifactModality.TABLE, Path('/tmp/data.csv'))
    ])
    assert mocked_client['minio_client']().fput_object.call_count == 3
