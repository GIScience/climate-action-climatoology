import uuid
from abc import abstractmethod, ABC
from typing import List
from uuid import UUID

from minio import Minio

from climatoology.base.operator import Artifact


class Storage(ABC):

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


class MinioStorage(Storage):

    def __init__(self, host: str, port: int, access_key: str, secret_key: str):
        """Create a MinIO connection instance.

        :param host: MinIO instance host
        :param port: MinIO instance port
        :param access_key: MinIO instance access key (generate in management console)
        :param secret_key: MinIO instance secret (generate in management console)
        """
        self.client = Minio(endpoint=f'{host}:{port}', access_key=access_key, secret_key=secret_key)

    def save(self, artifact: Artifact) -> UUID:
        store_id = uuid.uuid4()
        bucket = artifact.modality.name.lower()
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

        self.client.fput_object(bucket_name=bucket,
                                object_name=str(store_id),
                                file_path=str(artifact.file_path),
                                metadata={
                                    'correlation_uuid': artifact.correlation_uuid,
                                    'ext': artifact.file_path.suffix.lower()
                                })
        return store_id

    def save_all(self, artifacts: List[Artifact]) -> List[UUID]:
        return [self.save(artifact) for artifact in artifacts]
