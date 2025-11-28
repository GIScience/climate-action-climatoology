import mimetypes
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import geojson_pydantic
from minio import Minio, S3Error
from pydantic import BaseModel, ConfigDict, Field
from semver import Version

from climatoology.base.artifact import ArtifactEnriched, ArtifactModality
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.event import ComputationState
from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import Assets, PluginBaseInfo, _convert_icon_to_thumbnail

log = get_climatoology_logger(__name__)


# TODO: move this to computation.py module? I always search for it there (but that creates a circular import so the artifact creation methods would need to be moved)
class ComputationInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='forbid')

    correlation_uuid: UUID = Field(description='The unique identifier of the computation.', examples=[uuid.uuid4()])
    request_ts: datetime = Field(
        description='The timestamp at which the computation was requested.', examples=[datetime.now()]
    )
    deduplication_key: UUID = Field(
        description='A key identifying unique contributions in terms of content. It is a combination of multiple '
        'fields of the info that are used to deduplicate computations in combination with the '
        '`cache_epoch`.',
        examples=[uuid.uuid4()],
    )
    cache_epoch: Optional[int] = Field(
        description='The cache epoch identifies fixed time spans within which computations are '
        'valid. It can be used in combination with the `deduplication_key` to deduplicate non-expired computations. ',
        examples=[1234],
    )
    valid_until: datetime = Field(description='The human readable form of the `cache_epoch`', examples=[datetime.now()])
    params: Optional[dict] = Field(
        description='The final parameters used for the computation.',
        examples=[{'param_a': 1, 'optional_param_b': 'b'}],
    )
    requested_params: dict = Field(
        description='The raw parameters that were requested by the client', examples=[{'param_a': 1}]
    )
    aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties] = Field(
        description='The target area of interest of the computation.',
        examples=[
            geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
                **{
                    'type': 'Feature',
                    'properties': {'name': 'test_aoi', 'id': 'test_aoi_id'},
                    'geometry': {
                        'type': 'MultiPolygon',
                        'coordinates': [
                            [
                                [
                                    [0.0, 0.0],
                                    [0.0, 1.0],
                                    [1.0, 1.0],
                                    [0.0, 0.0],
                                ]
                            ]
                        ],
                    },
                }
            )
        ],
    )
    artifacts: List[ArtifactEnriched] = Field(
        description='List of artifacts produced by this computation.',
        examples=[
            ArtifactEnriched(
                name='Artifact One',
                modality=ArtifactModality.MARKDOWN,
                filename='example_file.md',
                summary='An example artifact.',
                correlation_uuid=uuid.uuid4(),
                rank=0,
            )
        ],
        default=[],
    )
    plugin_info: PluginBaseInfo = Field(
        description='Basic information on the plugin that produced the computation.',
        examples=[
            PluginBaseInfo(id='example_plugin', version=Version(0, 0, 1)),
        ],
    )
    status: Optional[ComputationState] = Field(
        description='The current status of the computation.', examples=[ComputationState.SUCCESS], default=None
    )
    message: Optional[str] = Field(description='A message accompanying the computation.', examples=[None], default=None)
    artifact_errors: dict[str, str] = Field(
        description='A dictionary of artifact names that were not computed successfully during the computation, with error messages if applicable.',
        examples=[{'First Indicator': 'Start date must be before end date', 'Last Indicator': ''}],
        default={},
    )


class DataGroup(Enum):
    DATA = 'DATA'
    METADATA = 'METADATA'
    ASSET = 'ASSET'


class AssetType(Enum):
    ICON = 'ICON'


ASSET_FILE_NAMES = {AssetType.ICON: 'ICON.png'}


class Storage(ABC):
    """A file storage wrapper class."""

    @staticmethod
    def generate_object_name(correlation_uuid: UUID, store_id: str) -> str:
        return f'{correlation_uuid}/{store_id}'

    @staticmethod
    def generate_asset_object_name(plugin_id: str, plugin_version: str, asset_type: AssetType) -> str:
        filename = ASSET_FILE_NAMES.get(asset_type)
        object_name = f'assets/{plugin_id}/{plugin_version}/{filename}'
        return object_name

    @abstractmethod
    def save(self, artifact: ArtifactEnriched, file_dir: Path) -> list[str]:
        """Save a single artifact in the object store.

        :param artifact: Operators' report creation process result
        :param file_dir: the path of the file to be saved
        :return: object ids in the underlying object store
        """
        pass

    @abstractmethod
    def save_all(self, artifacts: List[ArtifactEnriched], file_dir: Path) -> list[str]:
        """Save multiple artifacts in the object store.

        :param artifacts: Operators' report creation process results
        :param file_dir: the path of the file to be saved
        :return: collection of object ids in the underlying object store
        """
        pass

    @abstractmethod
    def fetch(self, correlation_uuid: UUID, store_id: str, file_name: str = None) -> Optional[Path]:
        """Fetch an object from the store.

        This will download an element to the file cache of the broker using file_name. If the element does not exist
        under the given correlation_uuid/store_id path, None is returned.

        :param correlation_uuid: The folder of the element in the store
        :param store_id: The element name
        :param file_name: The name of the file the object should be stored in (within the file cache directory)
        :return: The path to the file or None
        """
        pass

    @abstractmethod
    def write_assets(self, plugin_id: str, assets: Assets) -> Assets:
        """Write the assets to the object store.

        Note that existing assets will be overwritten but deprecated ones will not be deleted.

        :param plugin_id: Name of the plugin these assets belong to
        :param assets: Assets to store
        :return: object id in the underlying object store
        """
        pass


class MinioStorage(Storage):
    def __init__(
        self,
        host: str,
        port: int,
        access_key: str,
        secret_key: str,
        secure: bool,
        bucket: str,
        file_cache: Path = Path('/tmp'),
    ):
        """Create a MinIO connection instance.

        :param host: MinIO instance host
        :param port: MinIO instance port
        :param access_key: MinIO instance access key (generate in management console)
        :param secret_key: MinIO instance secret (generate in management console)
        :param secure: Determine whether utilize SSL during MinIO connection
        :param bucket: Target bucket name
        """
        self.client = Minio(endpoint=f'{host}:{port}', access_key=access_key, secret_key=secret_key, secure=secure)

        if not self.client.bucket_exists(bucket):
            log.info(f'Bucket {bucket} does not exist. Creating it.')
            self.client.make_bucket(bucket)

        self.__bucket = bucket
        self.__file_cache = file_cache

    def save(self, artifact: ArtifactEnriched, file_dir: Path) -> list[str]:
        log.debug(f'Save artifact {artifact.correlation_uuid}: {artifact.name} from {file_dir}/{artifact.filename}')

        object_name = Storage.generate_object_name(artifact.correlation_uuid, store_id=artifact.filename)
        content_type = mimetypes.guess_type(artifact.filename)[0]
        object_type = (
            DataGroup.METADATA.value if artifact.modality == ArtifactModality.COMPUTATION_INFO else DataGroup.DATA.value
        )
        metadata = {'Type': object_type}

        self.client.fput_object(
            bucket_name=self.__bucket,
            object_name=object_name,
            file_path=str(file_dir / artifact.filename),
            metadata=metadata,
            content_type=content_type,
        )
        store_ids = [artifact.filename]

        if artifact.attachments and artifact.attachments.display_filename:
            display_object_name = Storage.generate_object_name(
                artifact.correlation_uuid, store_id=artifact.attachments.display_filename
            )
            display_content_type = mimetypes.guess_type(artifact.attachments.display_filename)[0]
            self.client.fput_object(
                bucket_name=self.__bucket,
                object_name=display_object_name,
                file_path=str(file_dir / artifact.attachments.display_filename),
                metadata=metadata,
                content_type=display_content_type,
            )
            store_ids.append(artifact.attachments.display_filename)

        return store_ids

    def save_all(self, artifacts: List[ArtifactEnriched], file_dir: Path) -> list[str]:
        store_ids = []
        for artifact in artifacts:
            store_ids.extend(self.save(artifact=artifact, file_dir=file_dir))
        return store_ids

    def fetch(self, correlation_uuid: UUID, store_id: str, file_name: str = None) -> Optional[Path]:
        if not file_name:
            file_name = store_id
        file_path = self.__file_cache / file_name

        try:
            object_name = Storage.generate_object_name(correlation_uuid=correlation_uuid, store_id=store_id)
            log.debug(f'Downloading {object_name} from bucket {self.__bucket} to {file_path}')
            self.client.fget_object(
                bucket_name=self.__bucket,
                object_name=object_name,
                file_path=str(file_path),
            )
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise e
        return file_path

    def get_artifact_url(
        self, correlation_uuid: UUID, store_id: str, expires: timedelta = timedelta(days=1)
    ) -> Optional[str]:
        """Retrieve an objects pre-signed URL from the store.

        If the element does not exist under the given correlation_uuid/store_id path, None is returned.

        :param correlation_uuid: The folder of the element in the store
        :param store_id: The element name
        :param expires: The time to expiration of the retrieved URL
        :return: The pre-signed url of the file or None
        """
        object_name = Storage.generate_object_name(correlation_uuid=correlation_uuid, store_id=store_id)
        return self._get_object_url(object_name=object_name, expires=expires)

    def get_icon_url(self, plugin_id: str, expires: timedelta = timedelta(days=1)) -> Optional[str]:
        object_name = Storage.generate_asset_object_name(
            plugin_id=plugin_id, plugin_version='latest', asset_type=AssetType.ICON
        )
        return self._get_object_url(object_name=object_name, expires=expires)

    def _get_object_url(self, object_name: str, expires: timedelta) -> Optional[str]:
        try:
            url = self.client.presigned_get_object(
                bucket_name=self.__bucket,
                object_name=object_name,
                expires=expires,
            )
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise e
        return url

    def write_assets(self, plugin_id: str, assets: Assets) -> Assets:
        icon_filename = self._synch_icon(icon_path=assets.icon, plugin_id=plugin_id)

        new_assets = Assets(icon=icon_filename)

        return new_assets

    def _synch_icon(self, icon_path: str, plugin_id: str) -> str:
        object_name = Storage.generate_asset_object_name(
            plugin_id=plugin_id, plugin_version='latest', asset_type=AssetType.ICON
        )
        binary_icon = _convert_icon_to_thumbnail(Path(icon_path))
        self.client.put_object(
            bucket_name=self.__bucket,
            object_name=object_name,
            data=binary_icon,
            metadata={'Type': DataGroup.ASSET.value},
            length=binary_icon.getbuffer().nbytes,
        )
        return object_name
