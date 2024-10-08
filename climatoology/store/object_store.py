import json
import logging
import uuid
from abc import abstractmethod, ABC
from datetime import timedelta
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, Optional
from uuid import UUID

from minio import Minio, S3Error

from climatoology.base.artifact import ArtifactModality, _Artifact

log = logging.getLogger(__name__)


class DataGroup(Enum):
    DATA = 'DATA'
    METADATA = 'METADATA'


COMPUTATION_INFO_FILENAME: str = 'metadata.json'


class Storage(ABC):
    """A file storage wrapper class."""

    @staticmethod
    def generate_object_name(correlation_uuid: UUID, store_id: str) -> str:
        return f'{correlation_uuid}/{store_id}'

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
        else:
            cleaned_filename = artifact.file_path.name.encode(encoding='ascii', errors='ignore').decode()
            store_id = f'{uuid.uuid4()}_{cleaned_filename}'
        artifact.store_id = store_id

        object_name = Storage.generate_object_name(artifact.correlation_uuid, store_id)
        metadata_object_name = f'{object_name}.metadata.json'

        log.debug(f'Save artifact {artifact.correlation_uuid}: {artifact.name} at {store_id}')

        self.client.fput_object(
            bucket_name=self.__bucket,
            object_name=object_name,
            file_path=str(artifact.file_path),
            metadata={
                'Type': DataGroup.DATA.value,
                'Metadata-Object-Name': metadata_object_name,
            },
        )

        self._save_artifact_metadata(artifact, metadata_object_name, object_name)

        return store_id

    def _save_artifact_metadata(self, artifact: _Artifact, metadata_object_name: str, object_name: str) -> None:
        metadata = artifact.model_dump(exclude={'file_path'}, mode='json')
        metadata['file_path'] = str(artifact.file_path.name)

        with NamedTemporaryFile(mode='x') as metadata_file:
            json.dump(metadata, metadata_file, indent=None)
            metadata_file.flush()

            self.client.fput_object(
                bucket_name=self.__bucket,
                object_name=metadata_object_name,
                file_path=metadata_file.name,
                metadata={
                    'Type': DataGroup.METADATA.value,
                    'Data-Object-Name': object_name,
                },
            )

    def save_all(self, artifacts: List[_Artifact]) -> List[str]:
        return [self.save(artifact) for artifact in artifacts]

    def list_all(self, correlation_uuid: UUID) -> List[_Artifact]:
        artifacts = []

        objects = self.client.list_objects(
            bucket_name=self.__bucket,
            prefix=str(correlation_uuid),
            recursive=True,
            include_user_meta=True,
        )
        for obj in objects:
            if obj.metadata['X-Amz-Meta-Type'] == DataGroup.METADATA.value:
                metadata = None
                try:
                    metadata = self.client.get_object(
                        bucket_name=self.__bucket,
                        object_name=obj.object_name,
                    )
                    plugin_artifact = _Artifact.model_validate(metadata.json())
                    if plugin_artifact.modality == ArtifactModality.COMPUTATION_INFO:
                        continue
                    artifacts.append(plugin_artifact)
                finally:
                    if metadata:
                        metadata.close()
                        metadata.release_conn()

        log.debug(f'Found {len(artifacts)} artifacts for correlation_uuid {correlation_uuid}')

        return artifacts

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
