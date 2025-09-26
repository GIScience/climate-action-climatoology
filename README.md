# Climatoology

This package provides the background functionality to serve climate action plugins and interact with the climate action
architecture.
The [climate action framework](https://heigit.atlassian.net/wiki/spaces/CA/pages/170066046/Architecture) operates
through an event-bus infrastructure.
All product logic is encapsulated within plugins that use utilities for data acquisition.
Plugins on the other hand handle the event bus interaction.
They use operators to produce results,
so-called artifacts,
that provide climate action information to the user.

## Operator Creation

To create a new operator (or plugin) please refer to the documentation in
the [plugin blueprint repository](https://gitlab.heigit.org/climate-action/plugin-blueprint).

## Utilities

The following set of utilities is currently available.
In addition, you may use any external service or request further utilities by opening an issue in this repository.

### LULC classification

This utility can generate LULC classifications for arbitrary areas.
It is exposed via the [`LulcUtility`](climatoology/utility/api.py).
For a full documentation of the functionality
see [the respective repository](https://gitlab.heigit.org/climate-action/utilities/lulc-utility).

### Naturalness indices

This utility can generate spectral indices (e.g. the NDVI) derived from remote sensing data for arbitrary areas.
It is exposed via the [`NaturalnessUtility`](climatoology/utility/api.py).
For a full documentation of the functionality
see [the respective repository](https://gitlab.heigit.org/climate-action/utilities/naturalness-utility).

## User Realm

This package also provides the endpoints that connect the user realm via
the [API Gateway](https://gitlab.heigit.org/climate-action/api-gateway) to the user realm.

## Database Migration

Climatoology uses a PostgreSQL database to store information and results from computations.
It uses [alembic](https://alembic.sqlalchemy.org) to track and upgrade the database schema when required.

### Schema Definitions

The definitions of tables and relations are in [climatoology/store/database/](climatoology/store/database), and the
definitions for migrating between database versions are
in [climatoology/store/database/migration](climatoology/store/database/migration).
The migration scripts need to be kept in sync with the definition of tables & relations.

Therefore, when changing the table [models](climatoology/store/database/models) a migration file has to be generated.
The migration files need to be independent of the library version, so they should **not** include imports from
climatoology.

### Generating Migration Files

To generate a pre-filled migration file, first create a `.env.migration` file to define your database connection.
You can e.g. use the devel-database from
the [infrastructure](https://gitlab.heigit.org/climate-action/dev-ops/infrastructure) repository.
The database should have been initialised with `alembic` and up to date with the current model version (before the
latest change).
To make sure the database is up to date, run `poetry run alembic upgrade head`.

Then run `poetry run alembic revision --autogenerate -m "<headline of your change>"`.
You should then review the generated file in detail before committing it.

You can also view the migration history with `alembic history`.

## Install

This package is currently only available via the repository.
You need to have read-access to this repository, then run
`pip install git+ssh://git@gitlab.heigit.org:2022/climate-action/climatoology.git@{version/release/tag or branch}`.

## Contributing

This Package uses [poetry](https://python-poetry.org/) for environment management.
Run `poetry install --with test,dev` to create the environment.
Don't forget to run `poetry run pre-commit install` to activate the specified [pre-commit](https://pre-commit.com/)
hooks.

---
<img alt="HeiGIT Logo" src="docs/logo.png"  width="40%">
