# Climatoology

This package provides the background functionality to serve climate action plugins.
The [climate action framework](https://heigit.atlassian.net/wiki/spaces/CA/pages/170066046/Architecture) operates through an event-bus infrastructure.
All product logic is encapsulated within plugins that use utilities for data acquisition.
Plugins on the other hand handle the event bus interaction.
They use operators to produce results,
so-called artifacts,
that provide climate action information to the user.

## API

The package features a REST-API that serves as a gateway between the user realm and the architecture.
It provides endpoints to list available plugins, trigger computation tasks and retrieve computation results.
Each computation generates a correlation id that uniquely identifies a computation request.
Result retrieval is bound to these ids in a two-step procedure:

1. All results generated through a given id can be listed. The list remains empty as long as the computation is not finished or in case it failed.
2. The listed results (artifacts) provide a `store_uuid` which is a unique identifier for that element. The element can then be downloaded in a second API call.

For more information see the API documentation page linked below.

Yet, the swagger documentation interface does not well display the `/computation/` endpoint which provides a [WebSocket](https://en.wikipedia.org/wiki/WebSocket): `ws://localhost:8000/computation/` (trailing `/` is mandatory). The websocket will provide status updates on computation tasks. The optional `correlation_uuid` parameter allows you to filter events by a specific computation request. A 3-second heartbeat is required. To test the websocket you can use tools like [websockets-cli](https://pypi.org/project/websockets-cli/).


## Utilities

The following set of utilities is currently available. In addition, you may use any external service or request further utilities by opening an issue in this repository.

### LULC classification

This utility can generate LULC classifications for arbitrary areas.
It is exposed via the [`LulcUtility`](climatoology/utility/api.py).
For a full documentation of the functionality see [the respective repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/lulc-utility).

## Operator Creation

To create a new operator (or plugin) please refer to the documentation in the [plugin blueprint repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/plugin-blueprint).


## Install

This package is currently only available via the repository.
You need to have read-access to this repository, then run
`pip install git+ssh://git@gitlab.gistools.geog.uni-heidelberg.de:2022/climate-action/climatoology.git@v2.3.0`.

## Run

The package and its API are embedded in a full event-driven architecture.
It therefore requires multiple services such as [minIO](https://min.io/) and [RabbitMQ](https://www.rabbitmq.com/) to be available and the respective environment variables to be set.
The simplest way to do so, is using docker.
You can use the [infrastructure repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/infrastructure) to set up the architecture.
Afterward copy [`.env_template`](.env_template) to `.env` and fill in the necessary environment variables.

### Direct run

Set the environment variables

```shell
export $(cat .env)
```

then start the api

```shell
 poetry run python climatoology/app/api.py
```

and head to [localhost:8000](localhost:8000/docs) to check out the results.

Of course, you won't see much until you also launch a plugin that can answer your calls. You can try the [plugin-blueprint](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/plugin-blueprint) or any other plugin listed in the [infrastructure repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/infrastructure).

### Docker

The tool is also [Dockerised](Dockerfile):

```shell
docker build . --tag heigit/ca-api-gateway:devel
docker run --env-file .env --network=host heigit/ca-api-gateway:devel
```

and head to the link above.

To run behind a proxy, you can configure the root path using the environment variable `ROOT_PATH`.

#### Deploy

Build the image as described above. To push a new version to [Docker Hub](https://hub.docker.com/orgs/heigit) run

```shell
docker image push heigit/ca-api-gateway:devel
```


### Further Optional Parameters

| env var                    | description                                    |
|----------------------------|------------------------------------------------|
| MINIO_BUCKET               | the minio bucket to use for storage            |
| MINIO_SECURE               | set to True to enable SSL in Minio connections |
| FILE_CACHE                 | location where files are temporarily stored    |
| API_GATEWAY_APP_CONFIG_DIR | The directory holding configuration files      |
| API_GATEWAY_API_PORT       | The port, the api should start under           |
| LOG_LEVEL                  | The api logging level                          |

## Contributing

This Package uses [poetry](https://python-poetry.org/) for environment management. Run `poetry install --with test` to create the environment. Don't forget to run `pre-commit install` to activate the specified [pre-commit](https://pre-commit.com/) hooks.

---
<img src="docs/logo.png"  width="40%">
