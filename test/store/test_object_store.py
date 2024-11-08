import tempfile
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import minio.datatypes

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.store.object_store import DataGroup


def test_minio_save(mocked_object_store, general_uuid, default_artifact):
    with patch.object(tempfile._RandomNameSequence, attribute='characters', new='a') as _:
        store_id = mocked_object_store['minio_storage'].save(default_artifact)

    mocked_object_store['minio_client'].assert_called_once_with(
        endpoint='minio.test.org:9999',
        access_key='key',
        secret_key='secret',
        secure=True,
    )
    mocked_object_store['minio_client']().fput_object.assert_any_call(
        bucket_name='test_bucket',
        object_name=f'{general_uuid}/{store_id}',
        file_path=str(default_artifact.file_path),
        metadata={
            'Type': DataGroup.DATA.value,
            'Metadata-Object-Name': f'{general_uuid}/{store_id}.metadata.json',
        },
    )

    mocked_object_store['minio_client']().fput_object.assert_any_call(
        bucket_name='test_bucket',
        object_name=f'{general_uuid}/{store_id}.metadata.json',
        file_path='/tmp/tmpaaaaaaaa',
        metadata={
            'Type': DataGroup.METADATA.value,
            'Data-Object-Name': f'{general_uuid}/{store_id}',
        },
    )
    assert mocked_object_store['minio_client']().fput_object.call_count == 2


def test_minio_save_special_character_filename(mocked_object_store, general_uuid, default_artifact):
    artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent.parent / 'test_äöüfile.tiff',
        summary='Test summary',
    )
    with patch.object(tempfile._RandomNameSequence, attribute='characters', new='a') as _:
        store_id = mocked_object_store['minio_storage'].save(artifact)

    assert store_id.endswith('_test_file.tiff')


def test_minio_save_all(mocked_object_store, general_uuid, default_artifact):
    second_correlation_uuid = uuid.uuid4()
    second_plugin_artifact = _Artifact(
        name='test',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path('/tmp/text.txt'),
        summary='A test',
        description='A test file',
        correlation_uuid=second_correlation_uuid,
    )
    mocked_object_store['minio_storage'].save_all([default_artifact, second_plugin_artifact])
    assert mocked_object_store['minio_client']().fput_object.call_count == 4


def test_minio_list_all(mocked_object_store, general_uuid, default_artifact):
    return_mock = [
        minio.datatypes.Object(
            bucket_name='test_bucket',
            object_name=f'{general_uuid}/{general_uuid}',
            metadata={
                'X-Amz-Meta-Type': DataGroup.DATA.value,
                'X-Amz-Meta-Metadata-Object-Name': f'{general_uuid}/{general_uuid}.metadata.json',
            },
        ),
        minio.datatypes.Object(
            bucket_name='test_bucket',
            object_name=f'{general_uuid}/{general_uuid}.metadata.json',
            metadata={
                'X-Amz-Meta-Type': DataGroup.METADATA.value,
                'X-Amz-Meta-Data-Object-Name': f'{general_uuid}/{general_uuid}',
            },
        ),
    ]
    mocked_object_store['minio_client']().list_objects.return_value = iter(return_mock)
    mocked_object_store['minio_client']().get_object.return_value.json.return_value = default_artifact.model_dump(
        mode='json'
    )

    result = mocked_object_store['minio_storage'].list_all(general_uuid)
    assert result == [default_artifact]

    mocked_object_store['minio_client']().list_objects.assert_called_once_with(
        bucket_name='test_bucket',
        prefix=str(general_uuid),
        recursive=True,
        include_user_meta=True,
    )


def test_minio_fetch(mocked_object_store, general_uuid):
    store_id = f'{general_uuid}_test_file.tiff'
    _result = mocked_object_store['minio_storage'].fetch(general_uuid, store_id)
    mocked_object_store['minio_client']().fget_object.assert_called_once_with(
        bucket_name='test_bucket',
        object_name=f'{general_uuid}/{store_id}',
        file_path=f'/tmp/{store_id}',
    )


def test_minio_get_artifact_url(mocked_object_store, general_uuid):
    store_id = f'{general_uuid}_test_file.tiff'
    _result = mocked_object_store['minio_storage'].get_artifact_url(general_uuid, store_id)
    mocked_object_store['minio_client']().presigned_get_object.assert_called_once_with(
        bucket_name='test_bucket',
        object_name=f'{general_uuid}/{store_id}',
        expires=timedelta(days=1),
    )
