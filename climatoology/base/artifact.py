import uuid
from enum import Enum
from numbers import Number
from typing import Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import plotly.express as px
import plotly.io as pio
from affine import Affine
from numpy.typing import ArrayLike
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

from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import ArticleSource, Source, filter_sources

log = get_climatoology_logger(__name__)

COMPUTATION_INFO_FILENAME: str = 'metadata.json'
DISPLAY_FILENAME_SUFFIX = '-display'
ARTIFACT_OVERWRITE_FIELDS = {'filename'}

PLOTLY_TEMPLATE = pio.templates['plotly_white']
PLOTLY_TEMPLATE.layout.colorway = px.colors.qualitative.Safe
pio.templates.default = PLOTLY_TEMPLATE

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
    VECTOR_MAP_LAYER = 'VECTOR_MAP_LAYER'
    RASTER_MAP_LAYER = 'RASTER_MAP_LAYER'
    COMPUTATION_INFO = 'COMPUTATION_INFO'


class Attachments(BaseModel):
    legend: Optional[Legend] = Field(
        description='The legend attachment.',
        examples=[Legend(legend_data={'The red object': Color('red')})],
        default=None,
    )
    display_filename: Optional[str] = Field(
        description='The name of the file that stores the data optimised for front-end display.',
        examples=['my_first_artifact-display'],
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


class Artifact(ArtifactMetadata):
    """A result generated by a Plugin.

    It should not be instantiated directly. Convenience creation methods for each artifact type are provided. This
    assures format consistency and reduces necessary plugin code changes.
    """

    model_config = ConfigDict(from_attributes=True)

    modality: ArtifactModality = Field(description='The type of artefact created.', examples=[ArtifactModality.IMAGE])
    attachments: Attachments = Field(
        description='Additional information or files that are linked to this artifact.',
        examples=[Attachments(legend=Legend(legend_data={'The red object': Color('red')}))],
        default=Attachments(),
    )


class ArtifactEnriched(Artifact):
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


def enrich_artifacts(
    artifacts: list[Artifact], correlation_uuid: UUID, sources_library: dict[str, Source]
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
