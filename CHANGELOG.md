# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project mostly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://gitlab.heigit.org/climate-action/climatoology/-/compare/6.3.1...main)

### Added

- computations are now stored and deduplicated in the backend database in a searchable
  manner ([#154](https://gitlab.heigit.org/climate-action/climatoology/-/issues/154), [#110](https://gitlab.heigit.org/climate-action/climatoology/-/issues/110))
- a teaser sentence for plugins can now be specified to be displayed on hover in the
  front-end ([#162](https://gitlab.heigit.org/climate-action/climatoology/-/issues/162)).
- a user agent (application name) to the backand database connection
- a plugin state to transparently show the development state of a plugin to the
  end-user ([#163](https://gitlab.heigit.org/climate-action/climatoology/-/issues/163)).

### Changed

- allow all characters for plugin names. Yet, any non-alphabetical characters are removed from the id meaning that two
  plugins cannot have e.g. only a number in difference in the mane (e.g. plugin1 and
  plugin2) ([#169](https://gitlab.heigit.org/climate-action/climatoology/-/issues/169)).

## [6.3.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.3.1) - 2025-04-28

### Fixed

- the plugin initialisation now uses the correct backend-db connection string

## [6.3.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.3.1) - 2025-04-28

### Changed

- the plugin info is now stored in the backend database on plugin start. The `PluginInfoTask` was removed in favour of
  getting the info from the backend
  database ([#132](https://gitlab.heigit.org/climate-action/climatoology/-/issues/132))
- the per-artifact metadata files are removed as they were duplicating the content of the central metadata
  file ([#108](https://gitlab.heigit.org/climate-action/climatoology/-/issues/108)).

### Added

- the unit of measurement for the x and y-axis of the Chart2dPlot data can now be
  specified ([#94](https://gitlab.heigit.org/climate-action/climatoology/-/issues/94)).

## [6.2.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.2.0) - 2025-03-31

### Changed

- Removed `plugin_version` from `get_icon_url()` and storing under 'latest'
  instead ([#155](https://gitlab.heigit.org/climate-action/climatoology/-/issues/155))
- Validate input parameters in a standalone function and save full set of parameters in computation
  info ([#141](https://gitlab.heigit.org/climate-action/climatoology/-/issues/141))
- Customise error messages from input
  validation ([#128](https://gitlab.heigit.org/climate-action/climatoology/-/issues/128))
- Renamed `ComputeCommandStatus` to `ComputationState` and updated the options to use the celery state naming
- Added `resolution` to `NaturalnessWorkUnit` and as an input to `NaturalnessUtility.compute_vector`
- Edited the default `end_date` for `utility.api.TimeRange` objects to be the last calendar year

### Fixed

- Include ID field in geojson artifacts ([#140](https://gitlab.heigit.org/climate-action/climatoology/-/issues/140))
- GeoTiffs are now created with overviews ([#129](https://gitlab.heigit.org/climate-action/climatoology/-/issues/129))

### Added

- The info creation method now has two additional inputs to specify the demo parameters and demo AOI. This enables the
  platform to provide an indefinitely cached demo computation that can be provided for a quick demonstration of the
  plugins' capabilities ([#29](https://gitlab.heigit.org/climate-action/climatoology/-/issues/29)).
- A new artifact creation method for charts that accepts plotly figures as input and stores them into a json file that
  can be read by the front-end to be
  rendered ([#124](https://gitlab.heigit.org/climate-action/climatoology/-/issues/124))
- A new 'safe' artifact creation context manager which can be used to catch errors safely while generating artifacts,
  capturing the error messages in the computation info and as
  warnings ([#150](https://gitlab.heigit.org/climate-action/climatoology/-/issues/150))
- A `ClimatoologyUserError` class that plugins can use to relay failure messages to the end
  user ([#158](https://gitlab.heigit.org/climate-action/climatoology/-/issues/158))
- Record `'STARTED'` state at the start of computation tasks
- Added `expires` as an input to `get_icon_url()` to make this configurable by the API Gateway

## [6.1.2](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.1.2) - 2025-02-06

### Fixed

- Removed the trailing slash in the naturalness utility
  URLS ([#151](https://gitlab.heigit.org/climate-action/climatoology/-/issues/151))

## [6.1.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.1.1) - 2025-02-06

### Fixed

- Reduce the max raster size for the naturalness utility to handle SentinelHub errors
- Batch the inputs properly in Naturalness compute_vector in case multiple API requests are required
- Set the return index dtype from Naturalness compute_vector to match the input index dtype (
  Closes [#145](https://gitlab.heigit.org/climate-action/climatoology/-/issues/145))

## [6.1.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.1.0) - 2025-01-29

### Changed

- Operator compute tasks must return at least one artifact result

### Added

- Add naturalness utility ([#139](https://gitlab.heigit.org/climate-action/climatoology/-/issues/139))
- Updated list of acceptable colormaps for continuous
  legend ([#144](https://gitlab.heigit.org/climate-action/climatoology/-/issues/144))

## [6.0.2](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.0.2) - 2024-11-29

### Changed

- send a complete Feature over the event bus instead of separating the geometry and the properties

### Fixed

- the typing of the metadata aoi is corrected
- flush metadata content before uploading to object store

## [6.0.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.0.1) - 2024-11-28

### Changed

- the metadata file now only provides simplified information about the plugin, i.e. the id and the version

### Fixed

- added the aoi parameter to the metadata file written and served to and from the object
  store ([136](https://gitlab.heigit.org/climate-action/climatoology/-/issues/136))

## [6.0.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.0.0) - 2024-11-27

### Removed

- `RdYlGn` and `seismic` color palettes from the acceptable colormaps. The minimum and maximum colours of the palette
  were too similar.

### Changed

- the library now relies on [Celery](https://docs.celeryq.dev) for task management and
  execution ([#106](https://gitlab.heigit.org/climate-action/climatoology/-/issues/106)).
  The change was mostly API-neutral for now.
  This completely replaces the former `broker` module.
  Plugin developers can now make use of the simplified `start_plugin` method in the [
  `plugin`](climatoology/app/plugin.py) module.
  In addition, the settings for the execution were separated from the settings for the operator into a `CABaseSettings`
  that by default reads in the `.env.base` file for environment variables.


- made the `Info` object private (`_Info`) and introduced a generator method `generate_plugin_info`.
  This should be used instead of instantiating `_Info` directly (
  see [#31](https://gitlab.heigit.org/climate-action/climatoology/-/issues/31))
- operators now get two additional input variables `aoi` and `aoi_properties`.
  They replace the soft contract that was in place until now, to have these properties in the input model.
  They are now no longer allowed to be part of the input
  model ([#107](https://gitlab.heigit.org/climate-action/climatoology/-/issues/107),[#125](https://gitlab.heigit.org/climate-action/climatoology/-/issues/125))
- the base class for operators is now called `BaseOperator` (formerly `Operator`).
  This allows plugin devs to generically call their Operator
  `Operator` ([#71](https://gitlab.heigit.org/climate-action/climatoology/-/issues/71))
- plugin icons are now stored as static assets in the object store.
  This prevents handing around binary data as string in the `_Info`
  object ([#33](https://gitlab.heigit.org/climate-action/climatoology/-/issues/33)).
  The change also paves the way for [#29](https://gitlab.heigit.org/climate-action/climatoology/-/issues/29)


- require `purpose` and `methodology` for the plugin-info to be provided as markdown files
- write compact (geo)json instead of bloated, indented one (it's not meant to be
  human-readable) ([#102](https://gitlab.heigit.org/climate-action/climatoology/-/issues/102), [#76](https://gitlab.heigit.org/climate-action/climatoology/-/issues/76))
- updated and cleaned all dependencies.
  This includes breaking updates such as geopandas to v1.0.1.
  In case you use ohsome-py, make sure to update it to v0.4.0 for compatibility.

### Added

- create_geotiff_artifact can now handle masked
  arrays ([#67](https://gitlab.heigit.org/climate-action/climatoology/-/issues/67))
- `coolwarm` to acceptable colormaps
- artifacts can now be associated with 0...n tags for semantic
  grouping ([#58](https://gitlab.heigit.org/climate-action/climatoology/-/issues/58))
- introduced `Colormap` type for raster color map

### Fixed

- the geotiff artifact is now a proper Cloud Optimised Geotiff (
  COG) ([#104](https://gitlab.heigit.org/climate-action/climatoology/-/issues/104))
- error message on version mismatch when registering a new plugin.
  It now states the offending plugin library version instead of the APIs library version.
- timestamps used for reporting are now UTC ([#89](https://gitlab.heigit.org/climate-action/climatoology/-/issues/89))

## [5.2.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/5.2.0) - 2024-09-17

### Added

- function to generated pre-signed urls for artifacts in the `ObjectStore`
- option to specify the target file name, where an artifact file is fetched to via the `ObjectStore`

## [5.1.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/5.1.0) - 2024-09-09

### Fixed

- rasterio/gdal issue where the written geotiffs were no longer readable by the
  front-end ([#103](https://gitlab.heigit.org/climate-action/climatoology/-/issues/103))

### Added

- bikeability concern

## [5.0.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/5.0.0) - 2024-07-04

### Changed

- upgraded LULC utility to the latest response type (`LabelResponse`) for the label description endpoint (
  `get_class_legend`)

### Added

- new LULC utility fusion modes `only_corine` and `harmonized_corine`

## [4.0.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/4.0.1) - 2024-07-03

### Fixed

- artifact upload failures due to the limitations of the object store metadata when artifact filenames contained
  non-ASCII characters
- a failure of plugins to acknowledge a compute request if the compute run took more than 3 minutes by increasing the
  heartbeat timeout to 30min ([#100](https://gitlab.heigit.org/climate-action/climatoology/-/issues/100))

## [4.0.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/4.0.0) - 2024-05-21

### Changed

- `_Artifact` metadata is now written to and read from separate files in the object store circumventing the limitations
  of the object-stores metadata functionality ([#46](https://gitlab.heigit.org/climate-action/climatoology/-/issues/46))
- `Chart2dData` no longer normalises pie-chart y-values to sum to
  1 ([#73](https://gitlab.heigit.org/climate-action/climatoology/-/issues/73))

### Fixed

- Prevent compute request messages from being acknowledged twice towards the broker during computation, causing an
  error ([#69](https://gitlab.heigit.org/climate-action/climatoology/-/issues/69))

### Added

- GNU LGPLv3 License
- Computation metadata is now stored in a separate file together with the `_Artifact` files in the object store to make
  each object store computation directory information-complete
- `primary` boolean attribute to the `_Artifact` to distinguish between the main outputs of a plugin and additional
  information ([#81](https://gitlab.heigit.org/climate-action/climatoology/-/issues/81)).
- map legend information can now be stored alongside the
  geodata ([#55](https://gitlab.heigit.org/climate-action/climatoology/-/issues/55))

## [3.1.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/3.1.0) - 2024-02-27

### Fixed

- an issue where `Chart2dData` could not be created with exactly 3 or 4 data and color items

### Added

- the class description endpoint to the `LulcUtility`
- `MOBILITY_PEDESTRIAN` concern for the walkability plugin
- [ruff](https://docs.astral.sh/ruff/) formatting pre-commit
  hook ([#60](https://gitlab.heigit.org/climate-action/climatoology/-/issues/60))

---

## [3.0.3](https://gitlab.heigit.org/climate-action/climatoology/-/releases/3.0.3) - 2024-02-01

### Fixed

- an issue where the trailing `/` in the health-call on an HTTP-API (e.g. `LulcUtility`) would cause a redirect into
  no-mans-land in certain configurations

## [3.0.2](https://gitlab.heigit.org/climate-action/climatoology/-/releases/3.0.2) - 2024-01-31

### Fixed

- `Info` class containing the library version as `semver.Version` could not be serialised to JSON

## [3.0.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/3.0.1) - 2024-01-30

### Changed

- `RasterInfo` dataclass now accepts a `Tuple[R, G, B, A]` for the colormap to be directly usable with rasterio

## [3.0.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/3.0.0) - 2024-01-29

### Changed

- Versions no longer have a leading letter 'v'
- Renamed `LulcUtilityUtility` class
  to `LulcUtility` ([#8](https://gitlab.heigit.org/climate-action/climatoology/-/issues/8))
    - Renamed `root_url` variable to `path`
- Renamed `LULCWorkUnit` class to `LulcWorkUnit`
- The geojson convenience method now ignores any given index on the data as it does not contain relevant information
  that should be made available to the
  front-end ([#57](https://gitlab.heigit.org/climate-action/climatoology/-/issues/57))
- The input for the geotiff creation method is now a pydantic `RasterInfo` that holds all the related
  elements ([#64](https://gitlab.heigit.org/climate-action/climatoology/-/issues/64))

### Removed

- The code and dependencies related to the API-Gateway. It was not relevant for the function of the library and
  CA-plugin development. It now resides
  in [its own repository](https://gitlab.heigit.org/climate-action/api-gateway).
- The `Artifact`-Class is now 'hidden' and was renamed to `_Artifact`. This should prevent plugin developers from
  instantiating it directly and encourage the usage of the provided convenience
  methods. ([#12](https://gitlab.heigit.org/climate-action/climatoology/-/issues/12))

### Fixed

- a bug that prevented the usage of the convenience method for raster data with 2d
  arrays ([#56](https://gitlab.heigit.org/climate-action/climatoology/-/issues/56))
- an issue where any `ValidationError` raised by plugins was reported as being caused by wrong input. We now raise
  separate errors for issues that happen during input parsing and those that happen during
  computation ([#65](https://gitlab.heigit.org/climate-action/climatoology/-/issues/65))

### Added

- a health-check call to the API on `LulcUtility`
  initialisation ([plugin-blueprint#13](https://gitlab.heigit.org/climate-action/plugin-blueprint/-/issues/13))
- a CHANGELOG
- author information to the
  plugin `Info` ([#61](https://gitlab.heigit.org/climate-action/climatoology/-/issues/61))

---

## [v2.6.4](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.6.4)

### Fixed

- use the pydantic BaseModel as base class for the LULC work unit and correctly serialise it

## [v2.6.3](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.6.3)

### Changed

- use json instead of bytes for broker interaction
- return a JSON object for the correlation ID in the API gateway

### Fixed

- API gateway websocket resource leak
- broken connection to the API gateway on broker channel failure (#50)

### Added

- LULC Utility fusion mode to wrapper

## [v2.6.2](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.6.2)

### Changed

- make Artifact description optional

### Fixed

- remove deprecated pydantic `Color` class in favour of `Color` class from the new external
  library: [Color](https://github.com/pydantic/pydantic-extra-types/blob/main/pydantic_extra_types/color.py)

## [v2.6.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.6.1)

### Changed

- pie-chart values are now automatically scaled [0..1]
- compute tasks are always acknowledged

## [v2.6](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.6)

### Removed

- the user input parameters from the `Artifact`'s metadata

### Fixed

- multiple bugs related to the handling of colors of artifacts (#40, #44, #45)
- a bug where some fields would stay empty in the artifact metadata and would prevent the uploading to the filestore

### Added

- a plugin ID derived from the plugin name but sanitised (#42, #35)

## [v2.5](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.5)

### Added

- helper functions for `Artifact` creation

## [v2.4](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.4)

### Changed

- documentation on how to set up the plugin and infrastructure
- The citations of plugins are now parsed as a `List[dict]` instead of a `dict`. The key: ID is already present in the
  single objects.
- updated LULC Utility wrapper to latest version of LULC Utility API

### Added

- _mitigation_, _adaption_ and _waste_ as new plugin concerns
- CI/CD

## [v2.3](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.3)

### Changed

- we now use brokers asynchronously

### Added

- check if the library version in plugins matches the version used in the api gateway

## [v2.2] - Never happened

## [v2.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.1)

### Fixed

- docker build
- plugin info method not working as expected

### Added

- an ephemeral computation location

## [v2.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v2.0)

### Added

- API gateway functionality

---

## [v1.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/v1.0)

### Added

- first version of a plugin-based infrastructure
    - storage metadata annotation
    - plugin announcement method
- user input validation
- LULC utility wrapper