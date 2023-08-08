# Climatoology

This package provides the background functionality to serve climate action plugins.
The [climate action framework](https://heigit.atlassian.net/wiki/spaces/CA/pages/170066046/Architecture) operates through an event-bus infrastructure.
All product logic is encapsulated within plugins that use utilities for data acquisition.
Plugins on the other hand handle the event bus interaction.
They use operators to produces results,
so-called artifacts,
that provide climate action information to the user.

## Install

This Package uses [conda](https://docs.conda.io/en/latest/) for environment management. More precisely we use [mamba](https://mamba.readthedocs.io/en/latest/installation.html) as a drop-in replacement. Run `mamba env create -f environment.yaml` to create the environment.

## Utilities

### LULC classification

This utility can generate LULC classifications for arbitrary areas.
It is exposed via the [`LulcUtilityOperator`](climatoology/utility/api.py).
For a full documentation of the functionality see [the respective repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/lulc-utility).

---
<img src="docs/logo.png"  width="40%">
