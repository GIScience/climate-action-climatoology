# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project mostly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://gitlab.heigit.org/climate-action/climatoology/-/compare/6.4.4...main)

TODO: split changelog by plugin-dev relevant and climatoology-dev relevant

### Changed

- the project now requires python >=3.13.5 <3.14.0
- renamed
    - `LULC` utility module to `lulc`
    - `Naturalness` utility module to `naturalness`
    - `...Exception`s to `...Error`s
- de-deprecated basic chart artifact creation and use plotly for it
  (`create_chart_artifact`) ([#164](https://gitlab.heigit.org/climate-action/climatoology/-/issues/164))
- queues are created explicitly using the plugin id, enabling multiple workers to be online and to consume from the same
  queue ([#189](https://gitlab.heigit.org/climate-action/climatoology/-/issues/189))
- worker names now include the hostname (instead of `@_`), enabling multiple workers to be online yet still
  differentiated from each other
- `create_geojson_artifact` now requires a GeoDataFrame as input data and includes extra data columns in the artifact
  result ([#205](https://gitlab.heigit.org/climate-action/climatoology/-/issues/205))
- allowed PNG icons and made PNG the default for
  icons ([#114](https://gitlab.heigit.org/climate-action/climatoology/-/issues/114), [#179](https://gitlab.heigit.org/climate-action/climatoology/-/issues/179))
- `teaser` and `demo_config` are now required for the plugin info
- moved from [psycopg2 to psycopg3](https://www.psycopg.org) as db
  engine ([#222](https://gitlab.heigit.org/climate-action/climatoology/-/issues/222))
- `Info` models (pydantic and DB) where augmented with adapters to accept `semver.Version` natively
- rename plugin info object `plugin_id` to `id` (the object should have the _Plugin_ name-part)
- `generate_plugin_info` now accepts a `DemoConfig` object and includes `name` so a descriptive demo name can be
  used ([#218](https://gitlab.heigit.org/climate-action/climatoology/-/issues/218))
- `create_geojson_artifact` and `create_geotiff_artifact` now accept `legend: Legend` instead
  of `legend_data: Union[ContinuousLegendData, Dict[str, Color]]` to enable users to provide a custom
  title ([#217](https://gitlab.heigit.org/climate-action/climatoology/-/issues/217))
- renamed `adjust_bounds` to `generate_bounds` and accept a GeoSeries as input instead of a bounding box,
  which enables ([#200](https://gitlab.heigit.org/climate-action/climatoology/-/issues/200))
    - If the resulting bounds would have a dimension of 0, instead extend the bounds to the east/north to ensure valid
      bounds are returned.
    - Filter the returned bounds to drop any bounds that do not intersect the input geometry space.
- LULC and Naturalness utility now accept polygonal geometries instead of bboxes. This allows them to limit their
  requests and save resources ([#200](https://gitlab.heigit.org/climate-action/climatoology/-/issues/200))
- the `_Artifact.store_id` field was removed in favour of the `_Artifact.filename` which in turn replaces
  `_Artifact.file_path`.
- the `Artifact` creation helper methods now bundle their common input variables into a common `ArtifactMetadata`
  object ([#240](https://gitlab.heigit.org/climate-action/climatoology/-/issues/240))
- the plugin info now takes in a library that can be subsetted for the info-sources (general sources of the plugin) but
  will also be reused for the artifact sources (see
  below) ([#224](https://gitlab.heigit.org/climate-action/climatoology/-/issues/224))
- the different stages of the `_Artifact` are now more visible through different class names that also present the
  enrichment cycle through inheritance.

### Fix

- the database being in an inconsistent state in the brief moments between a task finishing on the worker and the
  subsequent `on_success` or `on_failure` callbacks. This
  caused [#201](https://gitlab.heigit.org/climate-action/climatoology/-/issues/201) but also random
  test-failures.
- update all dependencies
- elements are uploaded to the object store with a more precise content
  type ([#165](https://gitlab.heigit.org/climate-action/utilities/lulc-utility/-/issues/165))
- plugin authors order is now preserved ([#204](https://gitlab.heigit.org/climate-action/climatoology/-/issues/204))
- artifacts now have a rank attribute to assert their order is
  preserved ([#204](https://gitlab.heigit.org/climate-action/climatoology/-/issues/204))

### Removed

- the `platform` module, which is now implemented in
  the [API Gateway](https://gitlab.heigit.org/climate-action/api-gateway)
  ([#184](https://gitlab.heigit.org/climate-action/climatoology/-/issues/184))
- `deduplicate_computations` from `CABaseSettings`, which is now available within `SenderSettings` in
  the [API Gateway](https://gitlab.heigit.org/climate-action/api-gateway)
- the deprecated `get_info_via_task` function, which is incompatible with the (changed) custom queue configuration
- deprecated columns `aoi_name`, `aoi_id` and `status` from computation
  table ([#197](https://gitlab.heigit.org/climate-action/climatoology/-/issues/197))
- `MinioStorage.list_all()` function, which is replaced
  by `BackendDatabase.list_artifacts()` ([#208](https://gitlab.heigit.org/climate-action/climatoology/-/issues/208))

### Added

- a dead letter queue to store expired
  messages ([#48](https://gitlab.heigit.org/climate-action/climatoology/-/issues/48))
- the functionality to automatically up- and downgrade the
  database including celery tables ([#170](https://gitlab.heigit.org/climate-action/climatoology/-/issues/170))
- a set of overVIEWs in the database for monitoring and
  reporting ([#187](https://gitlab.heigit.org/climate-action/climatoology/-/issues/187),
  [#215](https://gitlab.heigit.org/climate-action/climatoology/-/issues/215))
- sources given in the plugin info are now checked for completeness and compatibility with the
  front-end ([#118](https://gitlab.heigit.org/climate-action/climatoology/-/issues/118))
- `Info` now has the repository attribute that is automatically read from he pyproject.toml
- CI triggers for canary builds in downstream projects
- Artifacts can now have dedicated sources that are read from the central library defined during plugin info
  generation ([#202](https://gitlab.heigit.org/climate-action/climatoology/-/issues/202))
- Users can provide a custom title in the `Legend`
  object ([#217](https://gitlab.heigit.org/climate-action/climatoology/-/issues/217))
- Users can create legend data from a colormap using the `legend_data_from_colormap`
  function ([#217](https://gitlab.heigit.org/climate-action/climatoology/-/issues/217))
- `BackendDatabase` now has `list_artifacts()` to return a list of the artifacts for a
  computation ([#208](https://gitlab.heigit.org/climate-action/climatoology/-/issues/208))
- a custom logger that injects the celery task name and task id before log
  messages ([#153](https://gitlab.heigit.org/climate-action/climatoology/-/issues/153))

## [6.4.4](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.4.4) - 2025-10-08

### Changed

- make `resolution` a required input to `adjust_bounds` to ensure appropriate bounds will always be
  created ([#230](https://gitlab.heigit.org/climate-action/climatoology/-/issues/230))

### Fixed

- implementation of `adjust_bounds` to avoid recursion and instead pre-calculate the number of splits, and also to
  raise an error if the dimensions of the provided bounds are
  invalid ([#230](https://gitlab.heigit.org/climate-action/climatoology/-/issues/230))

## [6.4.3](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.4.3) - 2025-06-13

### Fixed

- add a `max_area_size` threshold to the utility for splitting bounds, to enable more refined control over processing
  limitations, as required
  by ([LULC Utility #83](https://gitlab.heigit.org/climate-action/utilities/lulc-utility/-/issues/83))

## [6.4.2](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.4.2) - 2025-06-04

### Changed

- deprecate computation state field in `ComputationInfo` and computation db
  table ([#190](https://gitlab.heigit.org/climate-action/climatoology/-/issues/190))
- switched to pytest-minio-mock to improve testability of minio
- `read_computation` is now returning the request timestamp instead of the computation
  timestamp ([#176](https://gitlab.heigit.org/climate-action/climatoology/-/issues/176)). The latter is disclosed in the
  computation message.

### Fixed

- make the AOI name and ID a request specific attribute instead of a computation attribute to allow user-defined names
  for the same AOI ([#183](https://gitlab.heigit.org/climate-action/climatoology/-/issues/183))
- make sure the icon and info are always overwritten and represent the platform
  state ([#178](https://gitlab.heigit.org/climate-action/climatoology/-/issues/178)).
- use a `NullPool` connection with our database engine to avoid intermittent database connection
  failures ([#181](https://gitlab.heigit.org/climate-action/climatoology/-/issues/181))

### Added

- store extended result information in the celery table of the backend database
- coverage reporting on tests included in CI ([194](https://gitlab.heigit.org/climate-action/climatoology/-/issues/194))

## [6.4.1](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.4.1) - 2025-05-13

### Added

- the platform now has the ability to set a Q-time for tasks as well as a time limit that overwrites the
  time limit set in the worker configuration.

## [6.4.0](https://gitlab.heigit.org/climate-action/climatoology/-/releases/6.4.0) - 2025-05-09

### Added

- computations are now stored and deduplicated for a given 'shelf life' (period of time) in the backend database in a
  searchable manner ([#154](https://gitlab.heigit.org/climate-action/climatoology/-/issues/154),
  [#110](https://gitlab.heigit.org/climate-action/climatoology/-/issues/110),
  [#157](https://gitlab.heigit.org/climate-action/climatoology/-/issues/157))
- a teaser sentence for plugins can now be specified to be displayed on hover in the
  front-end ([#162](https://gitlab.heigit.org/climate-action/climatoology/-/issues/162)).
- a user agent (application name) to the backend database connection
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

- rasterio/gdal issue where the written geo-tiffs were no longer readable by the
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