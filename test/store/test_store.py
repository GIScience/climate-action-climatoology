import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from climatoology.base.operator import Artifact, ArtifactModality
from climatoology.store.object_store import MinioStorage


@pytest.fixture()
def mocked_client():
    with patch('climatoology.store.object_store.Minio') as minio_client:
        minio_storage = MinioStorage(host='minio.test.org', port=9999, access_key='key', secret_key='secret')
        yield {'minio_storage': minio_storage, 'minio_client': minio_client}


def test_minio_save(mocked_client):
    correlation_id = uuid.uuid4()
    store_id = mocked_client['minio_storage'].save(
        Artifact(correlation_id, ArtifactModality.MAP_LAYER, Path('/tmp/map.tiff')))

    mocked_client['minio_client'].assert_called_once_with(endpoint='minio.test.org:9999', access_key='key',
                                                          secret_key='secret')
    mocked_client['minio_client']().fput_object.assert_called_once_with(bucket_name='map_layer',
                                                                        file_path='/tmp/map.tiff',
                                                                        metadata={
                                                                            'correlation_uuid': correlation_id,
                                                                            'ext': '.tiff'
                                                                        },
                                                                        object_name=str(store_id))


def test_minio_save_all(mocked_client):
    correlation_id = uuid.uuid4()
    mocked_client['minio_storage'].save_all([
        Artifact(correlation_id, ArtifactModality.MAP_LAYER, Path('/tmp/map.tiff')),
        Artifact(correlation_id, ArtifactModality.TEXT, Path('/tmp/text.txt')),
        Artifact(correlation_id, ArtifactModality.TABLE, Path('/tmp/data.csv'))
    ])
    assert mocked_client['minio_client']().fput_object.call_count == 3
