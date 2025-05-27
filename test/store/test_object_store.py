import tempfile
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import ANY, Mock, patch

from minio import S3Error
from urllib3 import HTTPResponse

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.info import Assets, _convert_icon_to_thumbnail
from climatoology.store.object_store import AssetType, DataGroup, Storage


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
        metadata={'Type': DataGroup.DATA.value},
    )

    assert mocked_object_store['minio_client']().fput_object.call_count == 1


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
    assert mocked_object_store['minio_client']().fput_object.call_count == 2


def test_minio_list_all(mocked_object_store, general_uuid, default_artifact, default_computation_info):
    mocked_object_store[
        'minio_client'
    ]().get_object.return_value.json.return_value = default_computation_info.model_dump(mode='json')

    result = mocked_object_store['minio_storage'].list_all(general_uuid)
    assert result == [default_artifact]

    mocked_object_store['minio_client']().get_object.assert_called_once_with(
        bucket_name='test_bucket', object_name=f'{general_uuid}/metadata.json'
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


def test_get_icon_url(mocked_object_store):
    _ = mocked_object_store['minio_storage'].get_icon_url(plugin_id='test_plugin')
    mocked_object_store['minio_client']().presigned_get_object.assert_called_once_with(
        bucket_name='test_bucket',
        object_name='assets/test_plugin/latest/ICON.jpeg',
        expires=timedelta(days=1),
    )


def test_minio_synchronise_icon(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/big_testing_image.jpeg'))
    with patch(
        'climatoology.store.object_store._convert_icon_to_thumbnail', side_effect=_convert_icon_to_thumbnail
    ) as mocked_thumbnail_call:
        mocked_object_store['minio_storage'].synch_assets(
            plugin_id='test_plugin', plugin_version='0.0.1', assets=assets, overwrite=True
        )
        mocked_thumbnail_call.assert_called_once_with(Path(__file__).parent.parent / 'resources/big_testing_image.jpeg')
        mocked_object_store['minio_client']().put_object.assert_called_once_with(
            bucket_name='test_bucket',
            object_name='assets/test_plugin/latest/ICON.jpeg',
            data=ANY,
            metadata={'Type': DataGroup.ASSET.value},
            length=4723,
        )


def test_minio_synchronise_asset(mocked_object_store):
    stat_object_mock = Mock(
        side_effect=S3Error(
            code='NoSuchKey',
            message='Object does not exist, resource: /test-bucket/assets/test_plugin/latest/ICON.jpeg',
            resource='/test-bucket/assets/test_plugin/latest/ICON.jpeg',
            request_id='1809657212C91FBD',
            host_id='localhost',
            bucket_name='test_bucket',
            object_name='assets/test_plugin/latest/ICON.jpeg',
            response=HTTPResponse(),
        )
    )
    mocked_object_store['minio_client']().stat_object = stat_object_mock

    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.jpeg'))
    mocked_object_store['minio_storage'].synch_assets(
        plugin_id='test_plugin', plugin_version='0.0.1', assets=assets, overwrite=False
    )
    mocked_object_store['minio_client']().put_object.assert_called_once_with(
        bucket_name='test_bucket',
        object_name='assets/test_plugin/latest/ICON.jpeg',
        data=ANY,
        metadata={'Type': DataGroup.ASSET.value},
        length=654,
    )


def test_minio_synchronise_asset_existing(mocked_object_store):
    assets = Assets(icon='resources/test_icon.jpeg')
    mocked_object_store['minio_storage'].synch_assets(
        plugin_id='test_plugin', plugin_version='0.0.1', assets=assets, overwrite=False
    )

    mocked_object_store['minio_client']().put_object.assert_not_called()


def test_minio_synchronise_asset_existing_overwrite(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.jpeg'))
    mocked_object_store['minio_storage'].synch_assets(
        plugin_id='test_plugin', plugin_version='0.0.1', assets=assets, overwrite=True
    )
    mocked_object_store['minio_client']().put_object.assert_called_once_with(
        bucket_name='test_bucket',
        object_name='assets/test_plugin/latest/ICON.jpeg',
        data=ANY,
        metadata={'Type': DataGroup.ASSET.value},
        length=654,
    )


def test_minio_synchronise_asset_rewrites_asset_object(mocked_object_store):
    assets = Assets(icon='resources/test_icon.jpeg')
    new_assets = mocked_object_store['minio_storage'].synch_assets(
        plugin_id='test_plugin', plugin_version='0.0.1', assets=assets, overwrite=False
    )
    assert new_assets.icon == 'assets/test_plugin/latest/ICON.jpeg'


def test_generate_asset_object_name():
    computed_asset_object_name = Storage.generate_asset_object_name(
        plugin_id='test_plugin', plugin_version='latest', asset_type=AssetType.ICON
    )
    assert computed_asset_object_name == 'assets/test_plugin/latest/ICON.jpeg'
