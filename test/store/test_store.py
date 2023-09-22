import uuid
from pathlib import Path
from unittest.mock import patch

import minio.datatypes
import pytest

from climatoology.base.operator import ArtifactModality, Artifact
from climatoology.store.object_store import MinioStorage


@pytest.fixture
def mocked_client():
    with patch('climatoology.store.object_store.Minio') as minio_client:
        minio_storage = MinioStorage(host='minio.test.org',
                                     port=9999,
                                     access_key='key',
                                     secret_key='secret',
                                     secure=True,
                                     bucket='test_bucket')
        yield {'minio_storage': minio_storage, 'minio_client': minio_client}


def test_minio_save(mocked_client, general_uuid, default_artifact):
    store_uuid = mocked_client['minio_storage'].save(default_artifact)

    mocked_client['minio_client'].assert_called_once_with(endpoint='minio.test.org:9999',
                                                          access_key='key',
                                                          secret_key='secret',
                                                          secure=True)
    mocked_client['minio_client']().fput_object.assert_called_once_with(bucket_name='test_bucket',
                                                                        object_name=f'{general_uuid}/{store_uuid}',
                                                                        file_path='test_file.tiff',
                                                                        metadata={
                                                                            'Name': 'test_name',
                                                                            'Modality': 'MAP_LAYER',
                                                                            'Original-Filename': 'test_file.tiff',
                                                                            'Summary': 'Test summary',
                                                                            'Description': 'Test description',
                                                                            'Correlation-UUID': general_uuid,
                                                                            'Store-UUID': store_uuid,
                                                                            'Params': '{"test param key": "test param val"}'
                                                                        })


def test_minio_save_all(mocked_client, general_uuid, default_artifact):
    second_correlation_uuid = uuid.uuid4()
    second_plugin_artifact = Artifact(name='test',
                                      modality=ArtifactModality.TEXT,
                                      file_path=Path('/tmp/text.txt'),
                                      summary='A test',
                                      description='A test file',
                                      correlation_uuid=second_correlation_uuid,
                                      params={})
    mocked_client['minio_storage'].save_all([default_artifact, second_plugin_artifact])
    assert mocked_client['minio_client']().fput_object.call_count == 2


def test_minio_list_all(mocked_client, general_uuid, default_artifact):
    return_mock = minio.datatypes.Object(bucket_name='test_bucket',
                                         object_name=f'{general_uuid}/{general_uuid}',
                                         metadata={
                                             'X-Amz-Meta-Name': 'test_name',
                                             'X-Amz-Meta-Modality': 'MAP_LAYER',
                                             'X-Amz-Meta-Original-Filename': 'test_file.tiff',
                                             'X-Amz-Meta-Summary': 'Test summary',
                                             'X-Amz-Meta-Description': 'Test description',
                                             'X-Amz-Meta-Store-Uuid': str(general_uuid),
                                             'X-Amz-Meta-Params': '{"test param key": "test param val"}'
                                         })
    mocked_client['minio_client']().list_objects.return_value = iter([return_mock])

    result = mocked_client['minio_storage'].list_all(general_uuid)
    assert result == [default_artifact]

    mocked_client['minio_client']().list_objects.assert_called_once_with(bucket_name='test_bucket',
                                                                         prefix=str(general_uuid),
                                                                         recursive=True,
                                                                         include_user_meta=True)


def test_minio_fetch(mocked_client, general_uuid):
    store_uuid = uuid.uuid4()
    _result = mocked_client['minio_storage'].fetch(general_uuid, store_uuid, Path('test_file.tiff'))
    mocked_client['minio_client']().fget_object.assert_called_once_with(bucket_name='test_bucket',
                                                                        object_name=f'{general_uuid}/{store_uuid}',
                                                                        file_path=Path('/tmp') / 'test_file.tiff')
