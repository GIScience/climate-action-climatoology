import tempfile
from pathlib import Path
from unittest.mock import ANY, patch

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.info import Assets, _convert_icon_to_thumbnail
from climatoology.store.object_store import AssetType, DataGroup, Storage


def test_minio_save_and_fetch(mocked_object_store, general_uuid, default_artifact):
    assert len(list(mocked_object_store.client.list_objects('minio_test_bucket'))) == 0
    store_id = mocked_object_store.save(default_artifact)
    assert len(list(mocked_object_store.client.list_objects('minio_test_bucket', recursive=True))) == 1
    with tempfile.TemporaryDirectory() as tmpdirname:
        fetched_file = mocked_object_store.fetch(
            correlation_uuid=general_uuid, store_id=store_id, file_name=f'{tmpdirname}/test_file.md'
        )
        assert fetched_file.read_text() == '# Test'


def test_minio_save_special_character_filename(mocked_object_store, general_uuid, default_artifact):
    artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent.parent / 'resources/test_artifact_file_$p€ciöl.md',
        summary='Test summary',
    )
    with patch.object(tempfile._RandomNameSequence, attribute='characters', new='a') as _:
        store_id = mocked_object_store.save(artifact)

    assert store_id.endswith('_test_artifact_file_$pcil.md')


def test_minio_save_all(mocked_object_store, general_uuid, default_artifact, mocker):
    second_artifact = default_artifact.model_copy()
    saved_artifacts = [default_artifact, second_artifact]
    save_info_spy = mocker.spy(mocked_object_store.client, 'fput_object')
    mocked_object_store.save_all(saved_artifacts)

    assert save_info_spy.call_count == 2


def test_minio_get_artifact_url(mocked_object_store, general_uuid):
    store_id = f'{general_uuid}_test_file.tiff'
    result = mocked_object_store.get_artifact_url(general_uuid, store_id)
    assert result == f'https://minio.test:1000/minio_test_bucket/{general_uuid}/{general_uuid}_test_file.tiff'


def test_get_icon_url(mocked_object_store):
    result = mocked_object_store.get_icon_url(plugin_id='test_plugin')
    assert result == 'https://minio.test:1000/minio_test_bucket/assets/test_plugin/latest/ICON.jpeg'


def test_big_icon_gets_thumbnailed(mocked_object_store, mocker):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/big_testing_image.jpeg'))
    with patch(
        'climatoology.store.object_store._convert_icon_to_thumbnail', side_effect=_convert_icon_to_thumbnail
    ) as mocked_thumbnail_call:
        icon_put_spy = mocker.spy(mocked_object_store.client, 'put_object')
        mocked_object_store.write_assets(plugin_id='test_plugin', assets=assets)
        mocked_thumbnail_call.assert_called_once_with(Path(__file__).parent.parent / 'resources/big_testing_image.jpeg')
        icon_put_spy.assert_called_once_with(
            bucket_name='minio_test_bucket',
            object_name='assets/test_plugin/latest/ICON.jpeg',
            data=ANY,
            metadata={'Type': DataGroup.ASSET.value},
            length=4723,
        )


def test_minio_synchronise_asset(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.jpeg'))
    mocked_object_store.write_assets(plugin_id='test_plugin', assets=assets)
    assert mocked_object_store.client.stat_object(
        bucket_name='minio_test_bucket', object_name='assets/test_plugin/latest/ICON.jpeg'
    )


def test_minio_synchronise_asset_rewrites_asset_object(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.jpeg'))
    new_assets = mocked_object_store.write_assets(
        plugin_id='test_plugin',
        assets=assets,
    )
    assert new_assets.icon == 'assets/test_plugin/latest/ICON.jpeg'


def test_generate_asset_object_name():
    computed_asset_object_name = Storage.generate_asset_object_name(
        plugin_id='test_plugin', plugin_version='latest', asset_type=AssetType.ICON
    )
    assert computed_asset_object_name == 'assets/test_plugin/latest/ICON.jpeg'
