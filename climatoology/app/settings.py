from datetime import timedelta
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

CELERY_HOST_PLACEHOLDER = '_'


class CABaseSettings(BaseSettings):
    log_level: str = 'INFO'

    minio_host: str
    minio_port: int
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool = False

    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_password: str
    rabbitmq_vhost: str = ''

    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_database: str

    overwrite_assets: bool = False

    model_config = SettingsConfigDict(env_file='.env.base')

    @property
    def backend_connection_string(self) -> str:
        return f'db+postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}'

    @property
    def broker_connection_string(self) -> str:
        return f'amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{self.rabbitmq_vhost}'


class WorkerSettings(BaseSettings):
    worker_send_task_events: bool = True
    worker_concurrency: int = 4
    worker_direct: bool = True
    worker_prefetch_multiplier: int = 1
    task_time_limit: int = timedelta(hours=0.5).total_seconds()
    task_track_started: bool = True
    result_expires: Optional[int] = None

    model_config = SettingsConfigDict(env_file='.env.worker')


class SenderSettings(BaseSettings):
    assert_plugin_version: bool = True
    task_send_sent_event: bool = True
    result_expires: Optional[int] = None

    model_config = SettingsConfigDict(env_file='.env.sender')
