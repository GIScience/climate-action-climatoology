import tempfile
from pathlib import Path
from unittest.mock import ANY, patch

import pytest

from climatoology.base.artifact import Attachments
from climatoology.base.info import Assets, _convert_icon_to_thumbnail
from climatoology.store.object_store import AssetType, DataGroup, Storage

TEST_RESOURCES_DIR = Path(__file__).parent.parent / 'resources'


def test_minio_save_and_fetch(mocked_object_store, general_uuid, default_artifact_enriched):
    assert len(list(mocked_object_store.client.list_objects('minio_test_bucket'))) == 0
    store_id = mocked_object_store.save(default_artifact_enriched, file_dir=TEST_RESOURCES_DIR)
    assert len(list(mocked_object_store.client.list_objects('minio_test_bucket', recursive=True))) == 1
    with tempfile.TemporaryDirectory() as tmpdirname:
        fetched_file = mocked_object_store.fetch(
            correlation_uuid=general_uuid, store_id=store_id[0], file_name=f'{tmpdirname}/test_file.md'
        )
        assert fetched_file.read_text() == '# Test'


def test_minio_save_display_file(mocked_object_store, general_uuid, default_artifact_enriched):
    artifact = default_artifact_enriched.model_copy(deep=True)
    artifact.attachments = Attachments(display_filename='test_methodology.md')
    store_id = mocked_object_store.save(artifact, file_dir=TEST_RESOURCES_DIR)
    assert len(list(mocked_object_store.client.list_objects('minio_test_bucket', recursive=True))) == 2
    with tempfile.TemporaryDirectory() as tmpdirname:
        fetched_file = mocked_object_store.fetch(
            correlation_uuid=general_uuid, store_id=store_id[1], file_name=f'{tmpdirname}/test_file.md'
        )
        assert fetched_file.read_text() == 'This is a test base'


def test_minio_save_content_type(mocked_object_store, default_artifact_enriched, mocker):
    save_info_spy = mocker.spy(mocked_object_store.client, 'fput_object')
    mocked_object_store.save(default_artifact_enriched, file_dir=TEST_RESOURCES_DIR)

    save_info_spy.assert_called_once_with(
        bucket_name=ANY, content_type='text/markdown', file_path=ANY, metadata=ANY, object_name=ANY
    )


def test_minio_save_all(mocked_object_store, general_uuid, default_artifact_enriched, mocker):
    second_artifact = default_artifact_enriched.model_copy()
    saved_artifacts = [default_artifact_enriched, second_artifact]
    save_info_spy = mocker.spy(mocked_object_store.client, 'fput_object')
    mocked_object_store.save_all(saved_artifacts, file_dir=TEST_RESOURCES_DIR)

    assert save_info_spy.call_count == 2


def test_minio_get_artifact_url(mocked_object_store, general_uuid):
    store_id = 'test_file.tiff'
    result = mocked_object_store.get_artifact_url(general_uuid, store_id)
    assert result == f'https://test.host:1234/minio_test_bucket/{general_uuid}/test_file.tiff'


def test_get_icon_url(mocked_object_store):
    result = mocked_object_store.get_icon_url(plugin_id='test_plugin')
    assert result == 'https://test.host:1234/minio_test_bucket/assets/test_plugin/latest/ICON.png'


@pytest.mark.parametrize(
    'icon_filename,expected_length',
    [
        ('big_testing_image.jpeg', 806),
        ('big_testing_image.png', 125),
    ],
)
def test_big_icon_gets_thumbnailed(mocked_object_store, mocker, icon_filename, expected_length):
    icon_path = Path(__file__).parent.parent / 'resources' / icon_filename
    assets = Assets(icon=str(icon_path))
    with patch(
        'climatoology.store.object_store._convert_icon_to_thumbnail', side_effect=_convert_icon_to_thumbnail
    ) as mocked_thumbnail_call:
        icon_put_spy = mocker.spy(mocked_object_store.client, 'put_object')
        mocked_object_store.write_assets(plugin_id='test_plugin', assets=assets)
        mocked_thumbnail_call.assert_called_once_with(icon_path)
        icon_put_spy.assert_called_once_with(
            bucket_name='minio_test_bucket',
            object_name='assets/test_plugin/latest/ICON.png',
            data=ANY,
            metadata={'Type': DataGroup.ASSET.value},
            length=expected_length,
        )


def test_minio_synchronise_asset(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.png'))
    mocked_object_store.write_assets(plugin_id='test_plugin', assets=assets)
    assert mocked_object_store.client.stat_object(
        bucket_name='minio_test_bucket', object_name='assets/test_plugin/latest/ICON.png'
    )


def test_minio_synchronise_asset_rewrites_asset_object(mocked_object_store):
    assets = Assets(icon=str(Path(__file__).parent.parent / 'resources/test_icon.png'))
    new_assets = mocked_object_store.write_assets(
        plugin_id='test_plugin',
        assets=assets,
    )
    assert new_assets.icon == 'assets/test_plugin/latest/ICON.png'


def test_generate_asset_object_name():
    computed_asset_object_name = Storage.generate_asset_object_name(
        plugin_id='test_plugin', plugin_version='latest', asset_type=AssetType.ICON
    )
    assert computed_asset_object_name == 'assets/test_plugin/latest/ICON.png'
