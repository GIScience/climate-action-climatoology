import uuid
from enum import Enum
from numbers import Number
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import numpy as np
import plotly
import plotly.express as px
import plotly.io as pio
import rasterio
import shapely
from affine import Affine
from geopandas import GeoDataFrame
from numpy.typing import ArrayLike
from pandas import DataFrame, MultiIndex
from PIL.Image import Image
from plotly.graph_objs import Figure
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    confloat,
    conint,
    conlist,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic_extra_types.color import Color
from rasterio import CRS
from rasterio.enums import OverviewResampling
from rasterio.rio.overview import get_maximum_overview_level

from climatoology.base.computation import ComputationResources
from climatoology.base.info import ArticleSource, Source, filter_sources
from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)

COMPUTATION_INFO_FILENAME: str = 'metadata.json'

plotly_template = pio.templates['plotly_white']
plotly_template.layout.colorway = px.colors.qualitative.Safe
pio.templates.default = plotly_template

type Colormap = Dict[
    Number,
    Union[
        Tuple[conint(ge=0, le=255), conint(ge=0, le=255), conint(ge=0, le=255)],
        Tuple[conint(ge=0, le=255), conint(ge=0, le=255), conint(ge=0, le=255), conint(ge=0, le=255)],
    ],
]


ACCEPTABLE_COLORMAPS = (
    # uniform sequential
    'viridis',
    'plasma',
    # sequential
    'binary',
    'YlOrRd',
    'YlGn',
    # diverging
    'coolwarm',
    # miscellaneous
    'terrain',
)  # extend from https://matplotlib.org/stable/users/explain/colors/colormaps.html at will


class LegendType(Enum):
    DISCRETE = 'DISCRETE'
    CONTINUOUS = 'CONTINUOUS'


class ContinuousLegendData(BaseModel):
    cmap_name: str = Field(
        title='Color Map Name',
        description='The name of the colormap the colors where picked from. Must be a matplotlib colormap, '
        'see https://matplotlib.org/stable/users/explain/colors/colormaps.html.',
        examples=['plasma'],
    )
    ticks: Dict[str, confloat(ge=0, le=1)] = Field(
        title='Ticks',
        description='Label and location of the legend ticks. The key represents the label the tick should have. It can '
        'be a data value (e.g. 0.5), a data value string (e.g. 0.5°C) or a string (e.g. '
        '`low temperature`). The value defines the location of the ticks between 0 (lowest) and 1 '
        '(highest value on the legend scale).',
        examples=[
            {'low': 0, 'high': 1},
        ],
    )

    @field_validator('cmap_name')
    @classmethod
    def must_be_valid_cmap_name(cls, v: str) -> str:
        if v.removesuffix('_r') not in ACCEPTABLE_COLORMAPS:
            raise ValueError(f'{v} is not among the accepted colormaps.')
        return v


class Legend(BaseModel):
    title: Optional[str] = Field(
        title='Legend Title',
        description='A custom legend title to use instead of the artifact name.',
        default=None,
        examples=['Legend Title'],
    )
    legend_data: Union[Dict[str, Color], ContinuousLegendData] = Field(
        title='Legend Data',
        description='The data that is required to plot the legend. For a legend with distinct colors provide a '
        'dictionary mapping labels (str) to colors. For a continuous legend, use the ContinuousLegendData type.',
        examples=[
            {'The black void', Color('black').as_hex()},
            ContinuousLegendData(cmap_name='plasma', ticks={'low': 0, 'high': 1}),
        ],
    )

    @computed_field()
    def legend_type(self) -> LegendType:
        if isinstance(self.legend_data, dict):
            return LegendType.DISCRETE
        elif isinstance(self.legend_data, ContinuousLegendData):
            return LegendType.CONTINUOUS
        else:
            raise ValueError(f'Legend data type {type(self.legend_data)} not supported')

    @field_serializer('legend_data')
    def serialize_color(self, co: Union[Dict[str, Color], ContinuousLegendData], _info):
        if isinstance(co, dict):
            return {lab: c.as_hex() for lab, c in co.items()}
        else:
            return co.model_dump()


class ArtifactModality(Enum):
    """Available artifact types."""

    MARKDOWN = 'MARKDOWN'
    TABLE = 'TABLE'
    IMAGE = 'IMAGE'
    CHART_PLOTLY = 'CHART_PLOTLY'
    MAP_LAYER_GEOJSON = 'MAP_LAYER_GEOJSON'
    MAP_LAYER_GEOTIFF = 'MAP_LAYER_GEOTIFF'
    COMPUTATION_INFO = 'COMPUTATION_INFO'


class Attachments(BaseModel):
    legend: Optional[Legend] = Field(
        description='The legend attachment.',
        examples=[Legend(legend_data={'The red object': Color('red')})],
        default=None,
    )


class ArtifactMetadata(BaseModel):
    """Common attributes of artifacts across types."""

    name: str = Field(
        description='A short name for the artifact that could be used as an alias.',
        min_length=1,
        examples=['Nice Graphic'],
    )
    primary: bool = Field(
        description='Is this a primary artifact or does it exhibit additional or contextual information?',
        examples=[True],
        default=True,
    )
    tags: Set[str] = Field(
        description='A set of tags or topics that can be used to group artifacts semantically.  For example all '
        'artifacts related to one information aspect of a plugin may play together and could be grouped '
        '(e.g. a map and a plot).',
        examples=[{'Tag A'}],
        default=set(),
    )
    filename: str = Field(
        description='The name of the file that stores the artifact. Omit the extension.',
        examples=['my_first_artifact'],
        default=uuid.uuid4(),
    )
    summary: str = Field(
        description='A short description of the artifact that could be used in a caption.',
        min_length=1,
        examples=['This image shows A.'],
    )
    description: Optional[str] = Field(
        description='A long description of the generated output that may help users better understand the artifact.',
        min_length=1,
        examples=['This image shows A and was taken from B by C because of D.'],
        default=None,
    )
    sources: set[str] = Field(
        description='A list of bibtex source-keys that will be used to select the sources for this artifact from your '
        "library defined in the plugin's info method.",
        examples=[{'key1', 'foo2025'}],
        default=set(),
    )

    @field_validator('filename', mode='after')
    @classmethod
    def check_filename_ascii_compliance(cls, value: str) -> str:
        value.encode(encoding='ascii', errors='strict')
        return value


class _Artifact(ArtifactMetadata):
    """A result generated by a Plugin.

    It should not be instantiated directly. Convenience creation methods for each artifact type are provided. This
    assures format consistency and reduces necessary plugin code changes.
    """

    model_config = ConfigDict(from_attributes=True)

    modality: ArtifactModality = Field(description='The type of artefact created.', examples=[ArtifactModality.IMAGE])
    attachments: Optional[Attachments] = Field(
        description='Additional information or files that are linked to this artifact.',
        examples=[Attachments(legend=Legend(legend_data={'The red object': Color('red')}))],
        default=None,
    )


class ArtifactEnriched(_Artifact):
    correlation_uuid: UUID = Field(
        description='The correlation UUID of the computation that generated this artifact.',
        examples=[uuid.uuid4()],
    )
    rank: int = Field(description='Rank of the artifact within the computation.', examples=[0, 1, 2, 3], ge=0)
    sources: list[Source] = Field(
        description='A list of sources that were used to generate this artifact. ',
        examples=[
            [
                ArticleSource(
                    pages='14-15',
                    volume='2',
                    journal='J. Geophys. Res.',
                    year='1954',
                    title="Nothing Particular in this Year's History",
                    author='J. G. Smith and H. K. Weston',
                    ENTRYTYPE='article',
                    ID='smit54',
                )
            ]
        ],
        default=list(),
    )


class ChartType(Enum):
    SCATTER = 'SCATTER'
    LINE = 'LINE'
    BAR = 'BAR'
    PIE = 'PIE'


class Chart2dData(BaseModel):
    x: Union[conlist(float, min_length=1), conlist(str, min_length=1)] = Field(
        title='X values',
        description='Data values on the X axis. '
        'Must be the same length as '
        'y values. Only one of both '
        'can be a nominal variable '
        '(string). For Pie-Charts, '
        'all values must be '
        'positive numbers. They '
        'will be interpreted as '
        'share, i.e. divided by '
        'the sum.',
        examples=[['first', 'second', 'third']],
    )
    x_label: Optional[str] = Field(title='x label', description='The label for the x-axis', default=None)
    y: Union[conlist(float, min_length=1), conlist(str, min_length=1)] = Field(
        title='Y values',
        description='Data values on the Y axis. '
        'Must be the same length as '
        'x values. Only one of both '
        'can be a nominal variable '
        '(string).',
        examples=[[3, 2, 1]],
    )
    y_label: Optional[str] = Field(title='y label', description='The label for the y-axis', default=None)
    chart_type: ChartType = Field(
        title='Chart Type', description='The type of chart to be created.', examples=[ChartType.SCATTER]
    )
    color: Union[List[Color], Color] = Field(
        title='Chart Color',
        description='The color for the chart elements. If a list is given, it '
        'must be the same length as the data input. If only a single '
        'color is given, all elements will have the same color. Line-'
        'Charts accept only a single color (one line=one color).',
        examples=[['#590d08', '#590d08', '#590d08']],
        default='#590d08',
        validate_default=True,
    )

    @model_validator(mode='after')
    def check_length(self) -> 'Chart2dData':
        assert len(self.x) == len(self.y), 'X and Y data must be the same length.'

        if self.chart_type == ChartType.LINE:
            assert isinstance(self.color, Color), 'Line charts can only have a single color for the line.'
        else:
            assert isinstance(self.color, Color) or (len(self.color) == len(self.x)), (
                'Data and color lists must be the same length.'
            )

        return self

    @model_validator(mode='after')
    def check_type(self) -> 'Chart2dData':
        assert isinstance(self.x[0], float) or isinstance(self.y[0], float), (
            'Only one dimension can be nominal (a str).'
        )
        return self

    @model_validator(mode='after')
    def check_data(self) -> 'Chart2dData':
        if self.chart_type == ChartType.PIE:
            assert isinstance(self.y[0], float), 'Pie-chart Y-Axis must be numeric.'
            assert all(val >= 0 for val in self.y), 'Pie-chart Y-Data must be all positive.'
        return self

    @model_validator(mode='after')
    def explode_color(self) -> 'Chart2dData':
        if self.chart_type != ChartType.LINE and isinstance(self.color, Color):
            self.color = len(self.x) * [self.color]
        return self

    @field_serializer('color')
    def serialize_color_list(self, co: Union[Color, List[Color]], _info):
        if isinstance(co, Color):
            return co.as_hex()
        else:
            return [c.as_hex() for c in co]


def enrich_artifacts(
    artifacts: list[_Artifact], correlation_uuid: UUID, sources_library: dict[str, Source]
) -> list[ArtifactEnriched]:
    enriched_artifacts = []
    for rank, artifact in enumerate(artifacts):
        sources_filtered = filter_sources(sources_library=sources_library, source_keys=artifact.sources)
        enriched_artifact = ArtifactEnriched(
            **artifact.model_dump(exclude={'sources'}),
            correlation_uuid=correlation_uuid,
            rank=rank,
            sources=sources_filtered,
        )
        enriched_artifacts.append(enriched_artifact)
    return enriched_artifacts


ARTIFACT_OVERWRITE_FIELDS = {'filename'}


def create_markdown_artifact(
    text: str,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> _Artifact:
    """Create an artifact from text supporting Markdown formatting.

    You may use raw text or add any formatting to style your text using e.g. headings, emphasis or links.

    :param text: Text that can contain Markdown formatting.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    assert metadata.description is None, (
        'A markdown artifact should not have a description, the text itself is the long version of the summary.'
    )
    file_path = resources.computation_dir / f'{metadata.filename}.md'
    log.debug(f'Writing markdown file {file_path}')

    with open(file_path, 'x') as out_file:
        out_file.write(text)

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.MARKDOWN,
        filename=file_path.name,
    )
    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_table_artifact(
    data: DataFrame,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> _Artifact:
    """Create an artifact from a data frame.

    This will create a CSV file. Any index will be written as a normal column.

    :param data: Table to save as csv.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.csv'
    log.debug(f'Writing table {file_path}')

    data = data.reset_index()
    data.to_csv(file_path, header=True, index=False, index_label=False, mode='x')

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.TABLE,
        filename=file_path.name,
    )
    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_image_artifact(
    image: Image,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> _Artifact:
    """Create an artifact from a pillow image.

    This will create a PNG file.

    :param image: Image to save as PNG.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.png'
    log.debug(f'Writing image {file_path}')

    assert image.mode in ('1', 'L', 'LA', 'I', 'P', 'RGB', 'RGBA'), f'Image mode {image.mode} not supported.'

    image.save(file_path, format='PNG', optimize=True)

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.IMAGE,
        filename=file_path.name,
    )
    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_chart_artifact(
    data: Chart2dData,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> _Artifact:
    """Create a basic chart artifact using the plotly library.

    This will create a JSON file holding all information required to plot the defined chart.

    :param data: Chart data
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    log.debug(f'Creating basic {data.chart_type} chart artifact')

    match data.chart_type:
        case ChartType.SCATTER:
            fig = px.scatter(x=data.x, y=data.y, color_discrete_sequence=[c.as_hex() for c in data.color])
            fig = fig.update_traces(marker_size=10)

        case ChartType.LINE:
            fig = px.line(x=data.x, y=data.y, markers=True, color_discrete_sequence=[data.color.as_hex()])

        case ChartType.BAR:
            fig = px.bar(x=data.x, y=data.y, color_discrete_sequence=[c.as_hex() for c in data.color])

        case ChartType.PIE:
            fig = px.pie(values=data.x, names=data.y, color_discrete_sequence=[c.as_hex() for c in data.color])

        case _:
            raise ValueError(f'{data.chart_type} is not a supported chart type.')

    fig.update_layout({'xaxis': {'title': data.x_label}, 'yaxis': {'title': data.y_label}})
    result = create_plotly_chart_artifact(figure=fig, metadata=metadata, resources=resources)

    return result


def create_plotly_chart_artifact(
    figure: Figure,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> _Artifact:
    """Create a chart artifact from a custom plotly chart.

    This will create a JSON file holding all information required to plot the defined chart.

    :param figure: Plotly figure object.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.json'
    log.debug(f'Writing chart {file_path}')

    with open(file_path, 'x') as out_file:
        plotly.io.write_json(figure, out_file)

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.CHART_PLOTLY,
        filename=file_path.name,
    )
    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_geojson_artifact(
    data: GeoDataFrame,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
    color: str = 'color',
    label: str = 'label',
    legend: Optional[Legend] = None,
) -> _Artifact:
    """Create a vector data artifact.

    This will create a GeoJSON file containing all information in `data`.

    :param data: The Geodata. Must have an active geometry column and a CRS.
    :param color: Column name for the color values, defaults to `'color'`. Column must contain
      instances of `pydantic_extra_types.color.Color` only.
    :param label: Column name for the labels of the features, defaults to `'label'`.
    :param legend: Can be used to display a custom legend. If not provided, a distinct legend will be created from the
      unique combinations of labels and colors.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources of the plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    data = data.copy(deep=True)

    file_path = resources.computation_dir / f'{metadata.filename}.geojson'
    assert not file_path.exists(), (
        'The target artifact data file already exists. Make sure to choose a unique filename.'
    )
    log.debug(f'Writing vector dataset {file_path}.')

    assert data.active_geometry_name is not None, 'No active geometry column in data'
    assert data.crs is not None, 'No CRS set for data.'

    assert data[color].apply(isinstance, args=[Color]).all(), (
        f'Not all values in column {color} are of type pydantic_extra_types.color.Color'
    )
    data[color] = data[color].apply(lambda color_value: color_value.as_hex())

    assert not data[label].isna().any(), f'There are missing label values in column {label}'

    data = data.rename(columns={color: 'color', label: 'label'})

    if isinstance(data.index, MultiIndex):
        # Format the index the same way a tuple index would be rendered in `to_file()`
        data.index = "('" + data.index.map("', '".join) + "')"

    data = data.to_crs(4326)
    data.geometry = shapely.set_precision(data.geometry, grid_size=0.0000001)

    data.to_file(
        file_path,
        index=True,
        driver='GeoJSON',
        engine='pyogrio',
        layer_options={'SIGNIFICANT_FIGURES': 7, 'RFC7946': 'YES', 'WRITE_NAME': 'NO'},
    )

    if not legend:
        legend_df = data.groupby(['color', 'label']).size().index.to_frame(index=False)
        legend_df = legend_df.set_index('label')
        legend_data = legend_df.to_dict()['color']
        legend = Legend(legend_data=legend_data)

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        filename=file_path.name,
        attachments=Attachments(legend=legend),
    )

    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


class RasterInfo(BaseModel, arbitrary_types_allowed=True):
    data: ArrayLike = Field(
        title='Input data',
        description='The array of raster values to write to the image. Must be 2d or 3d.',
        examples=[[[1, 1], [1, 1]]],
    )
    crs: CRS = Field(
        title='CRS',
        description='The coordinate reference system.',
        examples=[CRS({'init': 'epsg:4326'}).to_string()],
    )
    transformation: Affine = Field(
        title='Transformation',
        description='An affine transformation. This is best read from an existing image or '
        'using https://github.com/rasterio/affine',
        examples=[Affine.identity()],
    )
    colormap: Optional[Colormap] = Field(
        title='Colormap',
        description='An optional colormap for easy '
        'display. It will be applied to the '
        'first layer of the image and '
        'resolves all possible array data '
        'values (key) to the respective '
        'RGB-color (value).',
        examples=[{1: Color('red').as_rgb_tuple()}],
        default=None,
    )


def create_geotiff_artifact(
    raster_info: RasterInfo,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
    legend: Optional[Legend] = None,
) -> _Artifact:
    """Create a raster data artifact.

    This will create a GeoTIFF file holding all information required to plot a simple map layer.

    :param raster_info: The RasterInfo object.
    :param legend: Can be used to display a custom legend. If not provided, a distinct legend will be created from the
      colormap, if it exists.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources of the plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.tiff'
    log.debug(f'Writing raster dataset {file_path}.')

    resampling = 'nearest'

    data_array = np.ma.masked_array(raster_info.data)

    assert np.issubdtype(data_array.dtype, np.number), 'Array must be numeric'
    assert min(data_array.shape) > 0, 'Input array cannot have zero length dimensions.'

    if data_array.ndim == 2:
        count = 1
        height = data_array.shape[0]
        width = data_array.shape[1]

        indexes = count
    elif data_array.ndim == 3:
        count = data_array.shape[0]
        height = data_array.shape[1]
        width = data_array.shape[2]

        indexes = list(range(1, count + 1))
    else:
        raise ValueError('Only 2 and 3 dimensional arrays are supported.')

    profile = {
        'driver': 'COG',
        'height': height,
        'width': width,
        'count': count,
        'dtype': data_array.dtype,
        'crs': raster_info.crs,
        'transform': raster_info.transformation,
    }

    with rasterio.open(file_path, mode='w', **profile) as out_map_file:
        out_map_file.write(data_array, indexes=indexes)

        max_overview_level = get_maximum_overview_level(width, height)
        overview_levels = [2**j for j in range(1, max_overview_level + 1)]
        out_map_file.build_overviews(overview_levels, OverviewResampling[resampling])
        out_map_file.update_tags(ns='rio_overview', resampling=resampling)

        if raster_info.colormap:
            assert data_array.dtype in (np.uint8, np.uint16), f'Colormaps are not allowed for dtype {data_array.dtype}.'
            out_map_file.write_colormap(1, raster_info.colormap)

    if not legend and raster_info.colormap:
        legend_data = legend_data_from_colormap(raster_info.colormap)
        legend = Legend(legend_data=legend_data)

    result = _Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename=file_path.name,
        attachments=Attachments(legend=legend) if legend else None,
    )

    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def legend_data_from_colormap(colormap: Colormap) -> Dict[str, Color]:
    """
    Create a legend from a colormap type.
    """
    legend_data = {}
    for color_id, color_values in colormap.items():
        if len(color_values) == 3:
            color = Color(color_values)
        else:
            r, g, b, a = color_values
            color = Color((r, g, b, a / 255))
        legend_data[str(color_id)] = color
    return legend_data
