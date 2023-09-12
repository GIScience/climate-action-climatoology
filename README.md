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

This package is currently only available via the repository. You need to have read-access to this repository.  Depending on your connection choice (https or ssh) run `pip install git+ssh://git@gitlab.gistools.geog.uni-heidelberg.de:2022/climate-action/climatoology.git@v1.0` or `pip install git+https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology.git@0v1.0`.

## Contributing

This Package uses [poetry](https://python-poetry.org/) for environment management. Run `poetry install --with test` to create the environment. Don't forget to run `pre-commit install` to activate the specified [pre-commit](https://pre-commit.com/) hooks.

---
<img src="docs/logo.png"  width="40%">
