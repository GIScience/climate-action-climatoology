# Climatoology

This package provides the background functionality to serve climate action plugins.
The [climate action framework](https://heigit.atlassian.net/wiki/spaces/CA/pages/170066046/Architecture) operates through an event-bus infrastructure.
All product logic is encapsulated within plugins that use utilities for data acquisition.
Plugins on the other hand handle the event bus interaction.
They use operators to produces results,
so-called artifacts,
that provide climate action information to the user.

## Utilities

The following set of utilities is currently available. In addition, you may use any external service or request further utilities by opening an issue in this repository.

### LULC classification

This utility can generate LULC classifications for arbitrary areas.
It is exposed via the [`LulcUtilityOperator`](climatoology/utility/api.py).
For a full documentation of the functionality see [the respective repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/lulc-utility).

## Operator Creation

To create a new operator (or plugin) please refer to the documentation in the [plugin blueprint repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/plugin-blueprint).


## Install

This package is currently only available via the repository. You need to have read-access to this repository.  Depending on your connection choice (https or ssh) run `pip install git+ssh://git@gitlab.gistools.geog.uni-heidelberg.de:2022/climate-action/climatoology.git@v2.0` or `pip install git+https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology.git@v2.0`.
Alternatively you can request an access token, then run `pip install git+https://climate-action:${GIT_PROJECT_TOKEN}@gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology.git@v2.0`.

## Run

The package and its API are embedded in a full event-driven architecture. It therefore requires multiple services such as [minIO](https://min.io/) and [RabbitMQ](https://www.rabbitmq.com/) to be available. To start these, see the [infrastructure repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/infrastructure).

The following environment variables need to be set. Copy [`.env_template`](.env_template) to `.env` and fill in your config

| env var           | description                                                                                   |
|-------------------|-----------------------------------------------------------------------------------------------|
| MINIO_HOST        |                                                                                               |
| MINIO_PORT        |                                                                                               |
| MINIO_ACCESS_KEY  | your minIO access key. you have to manually create it in the UI or request it from the admins |
| MINIO_SECRET_KEY  | same as the access key                                                                        |
| RABBITMQ_HOST     |                                                                                               |
| RABBITMQ_PORT     |                                                                                               |
| RABBITMQ_API_URL  | the URL to the RabbitMQ management API.                                                       |
| RABBITMQ_USER     | a user with read access on the management API                                                 |
| RABBITMQ_PASSWORD |                                                                                               |

Activate the environment, then run

```shell
export PYTHONPATH="gateway:$PYTHONPATH"
python climatoology/app/api.py
```

Check out the result at [localhost:8000](localhost:8000).

### Further Optional Parameters

| env var                    | description                                    |
|----------------------------|------------------------------------------------|
| MINIO_BUCKET               | the minio bucket to use for storage            |
| MINIO_SECURE               | set to True to enable SSL in Minio connections |
| FILE_CACHE                 | location where files are temporarily stored    |
| API_GATEWAY_APP_CONFIG_DIR | The directory holding configuration files      |
| API_GATEWAY_API_PORT       | The port, the api should start under           |

## Docker

The tool is also [Dockerised](Dockerfile):

```shell
docker build . --tag heigit/ca-api-gateway:devel
docker run --env-file .env --network=host heigit/ca-api-gateway:devel
```

and head to the link above.

To run behind a proxy, you can configure the root path using the environment variable `ROOT_PATH`.

### Deploy

Build the image as described above. To push a new version to [Docker Hub](https://hub.docker.com/orgs/heigit) run

```shell
docker image push heigit/ca-api-gateway:devel
```


## Contributing

This Package uses [poetry](https://python-poetry.org/) for environment management. Run `poetry install --with test` to create the environment. Don't forget to run `pre-commit install` to activate the specified [pre-commit](https://pre-commit.com/) hooks.

---
<img src="docs/logo.png"  width="40%">
