# Changelog

## [Unreleased]

## [4.0.0] - 2024-05-21

### Added

- GNU LGPLv3 License
- Computation metadata is now stored in a separate file together with the `_Artifact` files in the object store to make each object store computation directory information-complete
- `primary` boolean attribute to the `_Artifact` to distinguish between the main outputs of a plugin and additional information ([#81](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/81)).
- map legend information can now be stored alongside the geodata ([#55](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/55))

### Changed

- `_Artifact` metadata is now written to and read from separate files in the object store circumventing the limitations of the object-stores metadata functionality ([#46](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/46))
- `Chart2dData` no longer normalises pie-chart y-values to sum to 1 ([#73](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/73))

### Fixed

- Prevent compute request messages from being acknowledged twice towards the broker during computation, causing an error ([#69](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/69))

## [3.1.0] - 2024-02-27

### Added

- the class description endpoint to the `LulcUtility`
- `MOBILITY_PEDESTRIAN` concern for the walkability plugin
- [ruff](https://docs.astral.sh/ruff/) formatting pre-commit hook ([#60](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/60))

### Fixed

- an issue where `Chart2dData` could not be created with exactly 3 or 4 data and color items

---

## [3.0.3] - 2024-02-01

### Fixed

- an issue where the trailing `/` in the health-call on an HTTP-API (e.g. `LulcUtility`) would cause a redirect into no-mans-land in certain configurations

## [3.0.2] - 2024-01-31

### Fixed

- `Info` class containing the library version as `semver.Version` could not be serialised to JSON

## [3.0.1] - 2024-01-30

### Changed

- `RasterInfo` dataclass now accepts a `Tuple[R, G, B, A]` for the colormap to be directly usable with rasterio

## [3.0.0] - 2024-01-29

### Added

- a health-check call to the API on `LulcUtility`
  initialisation ([plugin-blueprint#13](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/plugin-blueprint/-/issues/13))
- a CHANGELOG
- author information to the
  plugin `Info` ([#61](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/61))

### Changed

- Versions no longer have a leading letter 'v'
- Renamed `LulcUtilityUtility` class
  to `LulcUtility` ([#8](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/8))
    - Renamed `root_url` variable to `path`
- Renamed `LULCWorkUnit` class to `LulcWorkUnit`
- The geojson convenience method now ignores any given index on the data as it does not contain relevant information
  that should be made available to the
  front-end ([#57](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/57))
- The input for the geotiff creation method is now a pydantic `RasterInfo` that holds all the related
  elements ([#64](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/64))

### Removed

- The code and dependencies related to the API-Gateway. It was not relevant for the function of the library and
  CA-plugin development. It now resides
  in [its own repository](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/api-gateway).
- The `Artifact`-Class is now 'hidden' and was renamed to `_Artifact`. This should prevent plugin developers from
  instantiating it directly and encourage the usage of the provided convenience
  methods. ([#12](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/12))

### Fixed

- a bug that prevented the usage of the convenience method for raster data with 2d
  arrays ([#56](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/56))
- an issue where any `ValidationError` raised by plugins was reported as being caused by wrong input. We now raise
  separate errors for issues that happen during input parsing and those that happen during
  computation ([#65](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology/-/issues/65))

---

## [v2.6.4]

### Fixed

- use the pydantic BaseModel as base class for the LULC work unit and correctly serialise it

## [v2.6.3]

### Added

- LULC Utility fusion mode to wrapper

### Changed

- use json instead of bytes for broker interaction
- return a JSON object for the correlation ID in the API gateway

### Fixed

- API gateway websocket resource leak
- broken connection to the API gateway on broker channel failure (#50)

## [v2.6.2]

### Changed

- make Artifact description optional

### Fixed

- remove deprecated pydantic `Color` class in favour of `Color` class from the new external
  library: [Color](https://github.com/pydantic/pydantic-extra-types/blob/main/pydantic_extra_types/color.py)

## [v2.6.1]

### Changed

- pie-chart values are now automatically scaled [0..1]
- compute tasks are always acknowledged

## [v2.6]

### Add

- a plugin ID derived from the plugin name but sanitised (#42, #35)

### Fixed

- multiple bugs related to the handling of colors of artifacts (#40, #44, #45)
- a bug where some fields would stay empty in the artifact metadata and would prevent the uploading to the filestore

### Removed

- the user input parameters from the `Artifact`'s metadata

## [v2.5]

### Added

- helper functions for `Artifact` creation

## [v2.4]

### Added

- _mitigation_, _adaption_ and _waste_ as new plugin concerns
- CI/CD

### Changed

- documentation on how to set up the plugin and infrastructure
- The citations of plugins are now parsed as a `List[dict]` instead of a `dict`. The key: ID is already present in the
  single objects.
- updated LULC Utility wrapper to latest version of LULC Utility API

## [v2.3]

### Added

- check if the library version in plugins matches the version used in the api gateway

### Changed

- we now use brokers asynchronously

## [v2.2] - Never happened

## [v2.1]

### Added

- an ephemeral computation location

### Fixed

- docker build
- plugin info method not working as expected

## [v2.0]

### Added

- API gateway functionality

---

## [v1.0]

### Added

- first version of a plugin-based infrastructure
    - storage metadata annotation
    - plugin announcement method
- user input validation
- LULC utility wrapper

# General

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project mostly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).