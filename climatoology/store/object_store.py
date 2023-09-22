import json
import uuid
from abc import abstractmethod, ABC
from pathlib import Path
from typing import List, Optional
from uuid import UUID

from minio import Minio, S3Error

from climatoology.base.operator import ArtifactModality, Artifact


class Storage(ABC):

    @staticmethod
    def generate_object_name(correlation_uuid: UUID, store_uuid: UUID) -> str:
        return f'{correlation_uuid}/{store_uuid}'

    @abstractmethod
    def save(self, artifact: Artifact) -> UUID:
        """Save a single artifact in the object store.

        :param artifact: Operators' report creation process result
        :return: object id in the underlying object store
        """
        pass

    @abstractmethod
    def save_all(self, artifacts: List[Artifact]) -> List[UUID]:
        """Save multiple artifacts in the object store.

        :param artifacts: Operators' report creation process results
        :return: collection of object ids in the underlying object store
        """
        pass

    @abstractmethod
    def list_all(self, correlation_uuid: UUID) -> List[Artifact]:
        """
        Acquire all artifacts created under a single correlator
        :param correlation_uuid: platform action correlator
        :return: artifact description list
        """
        pass

    @abstractmethod
    def fetch(self, correlation_uuid: UUID, store_uuid: UUID, file_name: Path = None) -> Optional[Path]:
        """Fetch an object from the store.

        This will download an element to the file cache of the broker using file_name. If the element does not exist
        under the given correlation_uuid/store_uuid path, None is returned.

        :param correlation_uuid: The folder of the element in the store
        :param store_uuid: The element name
        :param file_name: The name of the file should be downloaded to. If None it will be the store_uuid without file
        extension
        :return: The path to the file or None
        """
        pass


class MinioStorage(Storage):

    def __init__(self,
                 host: str,
                 port: int,
                 access_key: str,
                 secret_key: str,
                 secure: bool,
                 bucket: str,
                 file_cache: Path = Path('/tmp')):
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
            self.client.make_bucket(bucket)

        self.__bucket = bucket
        self.__file_cache = file_cache

    def save(self, artifact: Artifact) -> UUID:
        store_uuid = uuid.uuid4()

        self.client.fput_object(bucket_name=self.__bucket,
                                object_name=Storage.generate_object_name(artifact.correlation_uuid, store_uuid),
                                file_path=str(artifact.file_path),
                                metadata={
                                    'Name': artifact.name,
                                    'Modality': artifact.modality.name,
                                    'Original-Filename': str(artifact.file_path.name),
                                    'Summary': artifact.summary,
                                    'Description': artifact.description,
                                    'Correlation-UUID': artifact.correlation_uuid,
                                    'Store-UUID': store_uuid,
                                    'Params': json.dumps(artifact.params),
                                })
        return store_uuid

    def save_all(self, artifacts: List[Artifact]) -> List[UUID]:
        return [self.save(artifact) for artifact in artifacts]

    def list_all(self, correlation_uuid: UUID) -> List[Artifact]:
        artifacts = []

        objects = self.client.list_objects(bucket_name=self.__bucket,
                                           prefix=str(correlation_uuid),
                                           recursive=True,
                                           include_user_meta=True)
        for obj in objects:
            name = obj.metadata['X-Amz-Meta-Name']
            modality = ArtifactModality(obj.metadata['X-Amz-Meta-Modality'])
            file_path = obj.metadata['X-Amz-Meta-Original-Filename']
            summary = obj.metadata['X-Amz-Meta-Summary']
            description = obj.metadata['X-Amz-Meta-Description']
            params = json.loads(obj.metadata['X-Amz-Meta-Params'])
            store_uuid = UUID(obj.metadata['X-Amz-Meta-Store-Uuid'])
            plugin_artifact = Artifact(name=name,
                                       modality=modality,
                                       file_path=file_path,
                                       summary=summary,
                                       description=description,
                                       correlation_uuid=correlation_uuid,
                                       params=params,
                                       store_uuid=store_uuid)
            artifacts.append(plugin_artifact)

        return artifacts

    def fetch(self, correlation_uuid: UUID, store_uuid: UUID, file_name: Path = None) -> Optional[Path]:
        file_name = str(store_uuid) if not file_name else file_name
        file_path = self.__file_cache / file_name
        try:
            self.client.fget_object(bucket_name=self.__bucket,
                                    object_name=Storage.generate_object_name(correlation_uuid=correlation_uuid,
                                                                             store_uuid=store_uuid),
                                    file_path=file_path)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise e
        return file_path
