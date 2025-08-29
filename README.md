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
