import mimetypes
import warnings
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from climatoology.base.artifact import ArtifactEnriched, ArtifactModality
from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import Assets, AssetsFinal, _convert_icon_to_thumbnail

log = get_climatoology_logger(__name__)


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
    def write_assets(self, plugin_id: str, assets: Assets) -> AssetsFinal:
        """Write the assets to the object store.

        Note that existing assets will be overwritten but deprecated ones will not be deleted.

        :param plugin_id: Name of the plugin these assets belong to
        :param assets: Assets to store
        :return: object id in the underlying object store
        """
        pass


class S3Storage(Storage):
    def __init__(
        self,
        host: str,
        port: int,
        access_key: str,
        secret_key: str,
        secure: bool,
        bucket: str,
        region: str = 'eu-central-1',
        user_agent: Optional[str] = None,
        file_cache: Path = Path('/tmp'),
    ):
        """Create an S3 connection instance.

        :param host: S3 instance host
        :param port: S3 instance port
        :param access_key: S3 instance access key (generate in management console)
        :param secret_key: S3 instance secret (generate in management console)
        :param secure: Determine whether utilize SSL during S3 connection
        :param bucket: Target bucket name
        :param region: The S3 bucket location region, defaults to `eu-central-1`
        :param user_agent: The user-agent to use when connection to the S3 store. Will be prefixed with `climatoology-`.
          Defaults to `plugin`.
        :param file_cache: The path to the temporary location of dowloaded files
        """
        url = f'https://{host}:{port}' if secure else f'http://{host}:{port}'

        if user_agent is None:
            user_agent = 'plugin'
            warnings.warn(
                f'The user agent for S3 is not set. '
                f'Please use a reasonable user-agent that identifies your software. '
                f'Defaulting to "{user_agent}".'
            )
        user_agent = f'climatoology-{user_agent}'
        my_config = Config(region_name=region, user_agent=user_agent)
        self.client = boto3.client(
            service_name='s3',
            endpoint_url=url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=my_config,
        )

        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError:
            log.info(f'Bucket {bucket} does not exist. Creating it.')
            self.client.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region})

        self.__bucket = bucket
        self.__file_cache = file_cache

    def save(self, artifact: ArtifactEnriched, file_dir: Path) -> list[str]:
        log.debug(f'Save artifact {artifact.correlation_uuid}: {artifact.name} from {file_dir}/{artifact.filename}')

        object_name = Storage.generate_object_name(artifact.correlation_uuid, store_id=artifact.filename)
        content_type = mimetypes.guess_type(artifact.filename)[0] or 'application/octet-stream'
        object_type = (
            DataGroup.METADATA.value if artifact.modality == ArtifactModality.COMPUTATION_INFO else DataGroup.DATA.value
        )
        metadata = {'Type': object_type}

        self.client.upload_file(
            Bucket=self.__bucket,
            Key=object_name,
            Filename=str(file_dir / artifact.filename),
            ExtraArgs={'Metadata': metadata, 'ContentType': content_type},
        )
        store_ids = [artifact.filename]

        if artifact.attachments and artifact.attachments.display_filename:
            display_object_name = Storage.generate_object_name(
                artifact.correlation_uuid, store_id=artifact.attachments.display_filename
            )
            display_content_type = (
                mimetypes.guess_type(artifact.attachments.display_filename)[0] or 'application/octet-stream'
            )
            self.client.upload_file(
                Bucket=self.__bucket,
                Key=display_object_name,
                Filename=str(file_dir / artifact.attachments.display_filename),
                ExtraArgs={'Metadata': metadata, 'ContentType': display_content_type},
            )
            store_ids.append(artifact.attachments.display_filename)

        return store_ids

    def save_all(self, artifacts: List[ArtifactEnriched], file_dir: Path) -> list[str]:
        store_ids = []
        for artifact in artifacts:
            store_ids.extend(self.save(artifact=artifact, file_dir=file_dir))
        return store_ids

    def fetch(self, correlation_uuid: UUID, store_id: str, file_name: Optional[str] = None) -> Optional[Path]:
        if not file_name:
            file_name = store_id
        file_path = self.__file_cache / file_name

        object_name = Storage.generate_object_name(correlation_uuid=correlation_uuid, store_id=store_id)
        log.debug(f'Downloading {object_name} from bucket {self.__bucket} to {file_path}')
        try:
            self.client.download_file(
                Bucket=self.__bucket,
                Key=object_name,
                Filename=str(file_path),
            )
        except ClientError as e:
            log.debug(f'Object {object_name} not found', exc_info=e)
            return None
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
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.__bucket, 'Key': object_name},
                ExpiresIn=int(expires.total_seconds()),
            )
        except ClientError as e:
            log.debug(f'Object {object_name} not found', exc_info=e)
            return None
        return url

    def write_assets(self, plugin_id: str, assets: Assets) -> AssetsFinal:
        icon_filename = self._synch_icon(icon_path=assets.icon, plugin_id=plugin_id)

        new_assets = AssetsFinal(icon=icon_filename)

        return new_assets

    def _synch_icon(self, icon_path: Path, plugin_id: str) -> str:
        object_name = Storage.generate_asset_object_name(
            plugin_id=plugin_id, plugin_version='latest', asset_type=AssetType.ICON
        )
        binary_icon = _convert_icon_to_thumbnail(icon_path)

        content_type = mimetypes.guess_type(object_name)[0] or 'application/octet-stream'
        self.client.put_object(
            Bucket=self.__bucket,
            Key=object_name,
            Body=binary_icon,
            Metadata={'Type': DataGroup.ASSET.value},
            ContentLength=binary_icon.getbuffer().nbytes,
            ContentType=content_type,
        )
        return object_name
