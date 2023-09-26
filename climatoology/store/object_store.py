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
    def generate_object_name(correlation_uuid: UUID, store_id: str) -> str:
        return f'{correlation_uuid}/{store_id}'

    @abstractmethod
    def save(self, artifact: Artifact) -> str:
        """Save a single artifact in the object store.

        :param artifact: Operators' report creation process result
        :return: object id in the underlying object store
        """
        pass

    @abstractmethod
    def save_all(self, artifacts: List[Artifact]) -> List[str]:
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
    def fetch(self, correlation_uuid: UUID, store_id: str) -> Optional[Path]:
        """Fetch an object from the store.

        This will download an element to the file cache of the broker using file_name. If the element does not exist
        under the given correlation_uuid/store_id path, None is returned.

        :param correlation_uuid: The folder of the element in the store
        :param store_id: The element name
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

    def save(self, artifact: Artifact) -> str:
        store_id = f'{uuid.uuid4()}_{artifact.file_path.name}'

        self.client.fput_object(bucket_name=self.__bucket,
                                object_name=Storage.generate_object_name(artifact.correlation_uuid, store_id),
                                file_path=str(artifact.file_path),
                                metadata={
                                    'Name': artifact.name,
                                    'Modality': artifact.modality.name,
                                    'Original-Filename': str(artifact.file_path.name),
                                    'Summary': artifact.summary,
                                    'Description': artifact.description,
                                    'Correlation-UUID': artifact.correlation_uuid,
                                    'Store-ID': store_id,
                                    'Params': json.dumps(artifact.params),
                                })
        return store_id

    def save_all(self, artifacts: List[Artifact]) -> List[str]:
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
            store_id = obj.metadata['X-Amz-Meta-Store-Id']
            plugin_artifact = Artifact(name=name,
                                       modality=modality,
                                       file_path=file_path,
                                       summary=summary,
                                       description=description,
                                       correlation_uuid=correlation_uuid,
                                       params=params,
                                       store_id=store_id)
            artifacts.append(plugin_artifact)

        return artifacts

    def fetch(self, correlation_uuid: UUID, store_id: str) -> Optional[Path]:
        file_path = self.__file_cache/store_id
        try:
            self.client.fget_object(bucket_name=self.__bucket,
                                    object_name=Storage.generate_object_name(correlation_uuid=correlation_uuid,
                                                                             store_id=store_id),
                                    file_path=file_path)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise e
        return file_path
