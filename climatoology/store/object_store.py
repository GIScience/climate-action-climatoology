import uuid
from abc import abstractmethod, ABC
from pathlib import Path
from typing import List
from uuid import UUID

from minio import Minio

from climatoology.base.operator import Artifact, ArtifactModality


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

    def fetch_all(self, correlation_uuid: UUID, target_dir: Path) -> List[Artifact]:
        """
        Acquire all artifacts created under a single correlator
        :param correlation_uuid: platform action correlator
        :param target_dir: directory where downloaded files will be stored
        :return: downloaded artifact list
        """
        pass


class MinioStorage(Storage):

    def __init__(self, host: str, port: int, access_key: str, secret_key: str, secure: bool, bucket: str):
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

    def save(self, artifact: Artifact) -> UUID:
        store_uuid = uuid.uuid4()

        self.client.fput_object(bucket_name=self.__bucket,
                                object_name=f'{artifact.correlation_uuid}/{store_uuid}',
                                file_path=str(artifact.file_path),
                                metadata={
                                    'Correlation-UUID': artifact.correlation_uuid,
                                    'Original-Filename': str(artifact.file_path.name),
                                    'Modality': artifact.modality.name
                                })
        return store_uuid

    def save_all(self, artifacts: List[Artifact]) -> List[UUID]:
        return [self.save(artifact) for artifact in artifacts]

    def fetch_all(self, correlation_uuid: UUID, target_dir: Path) -> List[Artifact]:
        artifacts = []

        objects = self.client.list_objects(bucket_name=self.__bucket, prefix=str(correlation_uuid), recursive=True, include_user_meta=True)
        for obj in objects:
            file_path = target_dir / obj.metadata['X-Amz-Meta-Original-Filename']
            modality = ArtifactModality(obj.metadata['X-Amz-Meta-Modality'])
            self.client.fget_object(bucket_name=self.__bucket, object_name=obj.object_name, file_path=file_path)
            artifacts.append(Artifact(correlation_uuid, modality, file_path))

        return artifacts
