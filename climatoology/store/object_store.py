import datetime
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import geojson_pydantic
from minio import Minio, S3Error
from pydantic import BaseModel, ConfigDict, Field

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.event import ComputationState
from climatoology.base.info import Assets, _convert_icon_to_thumbnail

log = logging.getLogger(__name__)


class PluginBaseInfo(BaseModel):
    plugin_id: str
    plugin_version: str


class ComputationInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='forbid')

    correlation_uuid: UUID = Field(description='The unique identifier of the computation.', examples=[uuid.uuid4()])
    timestamp: datetime.datetime = Field(
        description='The timestamp of the computation.', examples=[datetime.datetime.now()]
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
    valid_until: datetime.datetime = Field(
        description='The human readable form of the `cache_epoch`', examples=[datetime.datetime.now()]
    )
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
    artifacts: List[_Artifact] = Field(
        description='List of artifacts produced by this computation.',
        examples=[
            _Artifact(
                name='Artifact One',
                modality=ArtifactModality.MARKDOWN,
                file_path=Path('/path/to/file/on/disc.md'),
                summary='An example artifact.',
            )
        ],
        default=[],
    )
    plugin_info: PluginBaseInfo = Field(
        description='Basic information on the plugin that produced the computation.',
        examples=[
            PluginBaseInfo(plugin_id='example_plugin', plugin_version='0.0.1'),
        ],
    )
    status: ComputationState = Field(
        description='The current status of the computation.', examples=[ComputationState.SUCCESS]
    )
    message: Optional[str] = Field(description='A message accompanying the computation.', examples=[None], default=None)
    artifact_errors: dict[str, str] = Field(
        description='A dictionary of artifact names that were not computed successfully during the computation, with error messages if applicable.',
        examples=[{'First Indicator': 'Start date must be before end date', 'Last Indicator': ''}],
        default={},
    )


COMPUTATION_INFO_FILENAME: str = 'metadata.json'


class DataGroup(Enum):
    DATA = 'DATA'
    METADATA = 'METADATA'
    ASSET = 'ASSET'


class AssetType(Enum):
    ICON = 'ICON'


ASSET_FILE_NAMES = {AssetType.ICON: 'ICON.jpeg'}


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
    def save(self, artifact: _Artifact) -> str:
        """Save a single artifact in the object store.

        :param artifact: Operators' report creation process result
        :return: object id in the underlying object store
        """
        pass

    @abstractmethod
    def save_all(self, artifacts: List[_Artifact]) -> List[str]:
        """Save multiple artifacts in the object store.

        :param artifacts: Operators' report creation process results
        :return: collection of object ids in the underlying object store
        """
        pass

    @abstractmethod
    def list_all(self, correlation_uuid: UUID) -> List[_Artifact]:
        """
        Acquire all artifacts created under a single correlator
        :param correlation_uuid: platform action correlator
        :return: artifact description list
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

    def save(self, artifact: _Artifact) -> str:
        if artifact.modality == ArtifactModality.COMPUTATION_INFO:
            store_id = COMPUTATION_INFO_FILENAME
            object_type = DataGroup.METADATA.value
        else:
            cleaned_filename = artifact.file_path.name.encode(encoding='ascii', errors='ignore').decode()
            store_id = f'{uuid.uuid4()}_{cleaned_filename}'
            object_type = DataGroup.DATA.value
        artifact.store_id = store_id

        object_name = Storage.generate_object_name(artifact.correlation_uuid, store_id)

        log.debug(f'Save artifact {artifact.correlation_uuid}: {artifact.name} at {store_id}')

        self.client.fput_object(
            bucket_name=self.__bucket,
            object_name=object_name,
            file_path=str(artifact.file_path),
            metadata={'Type': object_type},
        )

        return store_id

    def save_all(self, artifacts: List[_Artifact]) -> List[str]:
        return [self.save(artifact) for artifact in artifacts]

    def list_all(self, correlation_uuid: UUID) -> List[_Artifact]:
        metadata_object_name = Storage.generate_object_name(
            correlation_uuid=correlation_uuid, store_id=COMPUTATION_INFO_FILENAME
        )
        metadata = None
        try:
            metadata = self.client.get_object(
                bucket_name=self.__bucket,
                object_name=metadata_object_name,
            )
            metadata_obj = ComputationInfo.model_validate(metadata.json())
        finally:
            if metadata:
                metadata.close()
                metadata.release_conn()

        log.debug(f'Found {len(metadata_obj.artifacts)} artifacts for correlation_uuid {correlation_uuid}')

        return metadata_obj.artifacts

    def fetch(self, correlation_uuid: UUID, store_id: str, file_name: str = None) -> Optional[Path]:
        if not file_name:
            file_name = store_id
        file_path = self.__file_cache / file_name

        try:
            object_name = Storage.generate_object_name(correlation_uuid=correlation_uuid, store_id=store_id)
            log.debug(f'Download{object_name} from bucket {self.__bucket} to {file_path}')
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
