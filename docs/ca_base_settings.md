# CABaseSettings

These are settings that plugins using climatoology will need to provide as environment variables or in a file called `.env.base` in the current working directory on call of `climatoology.app.plugin.start_plugin`.

Plugins should come with a prefilled `.env.base_template` file that contains some development defaults to work with a local infrastructure setup.

## Logging

| Variable    | Description | Required | Default |
|-------------|-------------|----------|----------|
| `LOG_LEVEL` | Controlls the log level of the application. One of `INFO`, `WARNING`, `DEBUG`| False | `INFO` |

## S3

Configuration of the S3 object store for computed results.

| Variable    | Description | Required | Default |
|-------------|-------------|----------|---------|
| `MINIO_HOST`| Host URL of your S3 object store. | True | - |
| `MINIO_PORT`| Port of your S3 object store..| True | - |
| `MINIO_ACCESS_KEY`| Access key from S3 object store. On first startup, you will have to create this in your S3 object store management console.| True | - |
|`MINIO_SECRET_KEY`| Secret key from S3 object store. On first startup, you will have to create this in your S3 object store management console.| True | - |
| `MINIO_BUCKET`| Name of the target S3 object store bucket.| True |
| `MINIO_SECURE`| Determine whether to use SSL to connect to S3 object store. | False | `False` |

##  RabbitMQ
| Variable    | Description | Required | Default |
|-------------|-------------|----------|---------|
| `RABBITMQ_HOST` | Host URI for TCP connection to RabbitMQ. | True | - |
| `RABBITMQ_PORT` | Port for the TCP connection to RabbitMQ. | True | - |
| `RABBITMQ_USER` | User name for the connection to RabbitMQ. | True | - |
|`RABBITMQ_PASSWORD` | Password for the connection to RabbitMQ. | True | - |
| `RABBITMQ_VHOST` | Virtual host for the connection to RabbitMQ. | False | `''` |

More details can be found at [https://www.rabbitmq.com/docs/uri-spec](https://www.rabbitmq.com/docs/uri-spec)

## Celery
| Variable    | Description | Required | Default |
|-------------|-------------|----------|---------|
|`DEADLETTER_CHANNEL_NAME`| Name for the kombu deadletter queue. | False | `'deadletter'`|
|`DEADLETTER_EXCHANGE_NAME`| Name for the kombu deadletter exchange. | False | `'dlx'` |

## Postgres
| Variable    | Description | Required | Default |
|-------------|-------------|----------|---------|
|`POSTGRES_HOST`| Host URI for postgres instance. | True | - |
|`POSTGRES_PORT`| Port for postgres instance. | True | - |
|`POSTGRES_DATABASE`| Identifier for the postgres database. | True | - |
|`POSTGRES_USER` | User for the postgres database connection | True | - |
|`POSTGRES_PASSWORD` | Password for the postgres database connection | True | - |


