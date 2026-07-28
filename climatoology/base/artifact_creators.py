from copy import deepcopy
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import plotly
import rasterio
import shapely
from geopandas import GeoDataFrame
from numpy.ma import MaskedArray
from pandas import DataFrame, MultiIndex
from PIL.Image import Image
from plotly import express as px
from plotly.graph_objs import Figure
from pydantic_extra_types.color import Color
from rasterio import CRS
from rasterio.enums import OverviewResampling, Resampling
from rasterio.rio.overview import get_maximum_overview_level
from rasterio.warp import calculate_default_transform, reproject

from climatoology.base.artifact import (
    ARTIFACT_OVERWRITE_FIELDS,
    DISPLAY_FILENAME_SUFFIX,
    Artifact,
    ArtifactMetadata,
    ArtifactModality,
    Attachments,
    Chart2dData,
    ChartType,
    Colormap,
    Legend,
    RasterInfo,
    log,
)
from climatoology.base.computation import ComputationResources


def create_markdown_artifact(
    text: str,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
) -> Artifact:
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

    result = Artifact(
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
) -> Artifact:
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

    result = Artifact(
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
) -> Artifact:
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

    result = Artifact(
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
) -> Artifact:
    """Create a basic chart artifact using the plotly library.

    This will create a JSON file holding all information required to plot the defined chart.

    :param data: Chart data. For a pie chart, x holds the categories and y the corresponding values.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    log.debug(f'Creating basic {data.chart_type} chart artifact')

    chart_df = DataFrame(data={data.x_label: data.x, data.y_label: data.y, 'color': data.color})
    chart_df['color'] = chart_df['color'].apply(lambda x: x.as_hex())

    match data.chart_type:
        case ChartType.SCATTER:
            fig = px.scatter(
                chart_df,
                x=data.x_label,
                y=data.y_label,
                color='color',
                color_discrete_sequence=chart_df['color'],
                hover_data={'color': False},
            )
            fig = fig.update_traces(marker_size=10)
            fig.layout.showlegend = False

        case ChartType.LINE:
            fig = px.line(
                chart_df,
                x=data.x_label,
                y=data.y_label,
                color_discrete_sequence=chart_df['color'],
                markers=True,
            )

        case ChartType.BAR:
            fig = px.bar(
                chart_df,
                x=data.x_label,
                y=data.y_label,
                color='color',
                color_discrete_sequence=chart_df['color'],
                hover_data={'color': False},
            )
            fig.layout.showlegend = False

        case ChartType.PIE:
            fig = px.pie(
                chart_df,
                names=data.x_label,
                values=data.y_label,
                color_discrete_sequence=chart_df['color'],
            )

        case _:
            raise ValueError(f'{data.chart_type} is not a supported chart type.')

    result = create_plotly_chart_artifact(figure=fig, metadata=metadata, resources=resources, raw_data=chart_df)

    return result


def create_plotly_chart_artifact(
    figure: Figure,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
    raw_data: Optional[DataFrame] = None,
) -> Artifact:
    """Create a chart artifact from a custom plotly chart.

    This will create a JSON file holding all information required to plot the defined chart.

    :param figure: Plotly figure object.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources for this plugin.
    :param raw_data: The raw data that was used to create the chart.
      If provided, it will be added as (CSV) download data for the chart.

    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.json'
    log.debug(f'Writing chart {file_path}')

    with open(file_path, 'x') as out_file:
        plotly.io.write_json(figure, out_file)

    download_filename = file_path.name
    if raw_data is not None:
        download_artifact = create_table_artifact(data=raw_data, metadata=metadata, resources=resources)
        download_filename = download_artifact.filename

    result = Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.CHART_PLOTLY,
        filename=download_filename,
        attachments=Attachments(display_filename=file_path.name),
    )
    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_vector_artifact(
    data: GeoDataFrame,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
    color: str = 'color',
    label: str = 'label',
    legend: Optional[Legend] = None,
    pmtiles_lco: dict = None,
) -> Artifact:
    """Create a vector data artifact containing all information in `data`.

    :param data: The Geodata. Must have an active geometry column and a CRS as well as a column for color and labels.
    :param color: Column name for the color values, defaults to `'color'`. Column must contain
      instances of `pydantic_extra_types.color.Color` only.
    :param label: Column name for the labels of the features, defaults to `'label'`. Column must contain strings.
    :param legend: Can be used to display a custom legend. The keys of the legend data must include all observations in
      the `label` column in `data`. If not provided, a distinct legend will be created from the unique combinations of
      labels and colors.
    :param pmtiles_lco: Layer creation options forwarded to the PMTile creation, see
      https://gdal.org/en/stable/drivers/vector/pmtiles.html
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources of the plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    data = data.copy(deep=True)

    file_path = resources.computation_dir / f'{metadata.filename}.geojson'
    display_file_path = resources.computation_dir / f'{metadata.filename}{DISPLAY_FILENAME_SUFFIX}.pmtiles'

    check_vector_data(
        input_data=data, color_column_name=color, label_column_name=label, legend=legend, output_file_path=file_path
    )

    data, legend = transform_vector_data(
        input_data=data, color_column_name=color, label_column_name=label, legend=legend
    )

    log.debug(f'Writing download vector dataset {file_path}.')
    data.to_file(
        file_path,
        index=True,
        driver='GeoJSON',
        engine='pyogrio',
        layer_options={'SIGNIFICANT_FIGURES': 7, 'RFC7946': 'YES', 'WRITE_NAME': 'NO'},
        use_arrow=True,
    )

    create_vector_display_file(
        data=data, display_file_path=display_file_path, metadata=metadata, pmtiles_lco=pmtiles_lco
    )

    result = Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.VECTOR_MAP_LAYER,
        filename=file_path.name,
        attachments=Attachments(legend=legend, display_filename=display_file_path.name),
    )

    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def create_vector_display_file(
    data: GeoDataFrame, display_file_path: Path, metadata: ArtifactMetadata, pmtiles_lco: dict | None
):
    lco = {
        'NAME': metadata.name,
        'DESCRIPTION': metadata.summary,
        'MINZOOM': 0,
        'MAXZOOM': 15,
    }
    lco.update(pmtiles_lco or {})
    dsco = deepcopy(lco)
    dsco['TYPE'] = 'overlay'
    display_cols = ['color', 'label', data.active_geometry_name]
    display_data = data[display_cols]

    log.debug(f'Writing display vector dataset {display_file_path}.')
    display_data.to_file(
        display_file_path,
        driver='PMTiles',
        engine='pyogrio',
        dataset_options=dsco,
        layer_options=lco,
        use_arrow=True,
    )


def transform_vector_data(
    input_data: GeoDataFrame, color_column_name: str, label_column_name: str, legend: Legend | None
) -> tuple[GeoDataFrame, Legend | None]:
    input_data[color_column_name] = input_data[color_column_name].apply(lambda color_value: color_value.as_hex())
    input_data = input_data.rename(columns={color_column_name: 'color', label_column_name: 'label'})

    if not legend:
        legend_df = input_data.groupby(['color', 'label']).size().index.to_frame(index=False)
        legend_df = legend_df.set_index('label')
        legend_data = legend_df.to_dict()['color']
        legend = Legend(legend_data=legend_data)

    if isinstance(input_data.index, MultiIndex):
        input_data.index = input_data.index.to_flat_index()
    if (input_data.index.name and input_data.index.name != 'index') or not input_data.index.is_unique:
        input_data = input_data.reset_index(names=input_data.index.name or 'index_0')
    input_data.index = input_data.index.astype(str)

    input_data = input_data.to_crs(4326)
    input_data.geometry = shapely.set_precision(input_data.geometry, grid_size=0.0000001)

    return input_data, legend


def check_vector_data(
    input_data: GeoDataFrame,
    color_column_name: str,
    label_column_name: str,
    legend: Legend | None,
    output_file_path: Path,
):
    assert not output_file_path.exists(), (
        'The target artifact data file already exists. Make sure to choose a unique filename.'
    )

    assert input_data.active_geometry_name is not None, 'No active geometry column in data'
    assert input_data.crs is not None, 'No CRS set for data.'

    assert color_column_name in input_data.columns, (
        f'Dataset does not contain the color column "{color_column_name}", but it is required.'
    )
    assert input_data[color_column_name].apply(isinstance, args=[Color]).all(), (
        f'Not all values in column {color_column_name} are of type pydantic_extra_types.color.Color'
    )

    assert label_column_name in input_data.columns, (
        f'Dataset does not contain the label column "{label_column_name}", but it is required.'
    )
    assert not input_data[label_column_name].isna().any(), (
        f'There are missing label values in column {label_column_name}'
    )

    if legend and isinstance(legend.legend_data, dict):
        missing_legend_labels = set(input_data[label_column_name]).difference(set(legend.legend_data.keys()))
        assert len(missing_legend_labels) < 1, (
            f'The following labels are included in the data, but not in the legend: {missing_legend_labels}'
        )


def create_raster_artifact(
    raster_info: RasterInfo,
    metadata: ArtifactMetadata,
    resources: ComputationResources,
    display_square_pixels: bool = False,
    legend: Optional[Legend] = None,
) -> Artifact:
    """Create a raster data artifact.

    This will create a GeoTIFF file holding all information required to plot a simple map layer.

    :param raster_info: The RasterInfo object.
    :param legend: Can be used to display a custom legend. If not provided, a distinct legend will be created from the
      colormap, if it exists.
    :param display_square_pixels: By default, the display pixels will match the input pixels as close as possible.
      Because the display raster is a reprojected version of the original, this can mean that the display pixels are
      not squared. Set this variable to `True` to get square display pixels but potentially lose/alter the data in
      display.
    :param metadata: Standard Artifact attributes
    :param resources: The computation resources of the plugin.
    :return: The artifact that contains a path-pointer to the created file.
    """
    file_path = resources.computation_dir / f'{metadata.filename}.tiff'
    display_file_path = resources.computation_dir / f'{metadata.filename}{DISPLAY_FILENAME_SUFFIX}.tiff'
    log.debug(f'Writing raster dataset {file_path}.')

    resampling = 'nearest'

    data_array = raster_info.data
    if not isinstance(data_array, np.ma.MaskedArray):
        if np.issubdtype(data_array.dtype, np.integer):
            fill_value = np.iinfo(data_array.dtype).max
        else:
            fill_value = np.finfo(data_array.dtype).max
        data_array = MaskedArray(data_array, fill_value=fill_value)

    # Numpy may set a fill_value that is incompatible with the dtype of the data_array, so we check this
    assert data_array.dtype.type(data_array.fill_value) == data_array.fill_value, (
        'Array fill_value must be compatible with the dtype of the data'
    )

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

    if raster_info.colormap:
        assert data_array.dtype in (np.uint8, np.uint16), f'Colormaps are not allowed for dtype {data_array.dtype}.'

    raw_profile = {
        'driver': 'COG',
        'height': height,
        'width': width,
        'count': count,
        'dtype': data_array.dtype,
        'crs': raster_info.crs,
        'transform': raster_info.transformation,
        'nodata': data_array.fill_value,
    }
    max_overview_level = get_maximum_overview_level(width, height)
    overview_levels = [2**j for j in range(1, max_overview_level + 1)]

    if not legend and raster_info.colormap:
        legend_data = legend_data_from_colormap(raster_info.colormap)
        legend = Legend(legend_data=legend_data)

    write_raw_raster(
        data_array=data_array,
        file_path=file_path,
        indexes=indexes,
        overview_levels=overview_levels,
        colormap=raster_info.colormap,
        raw_profile=raw_profile,
        resampling=resampling,
    )

    if display_square_pixels:
        dst_width = None
        dst_height = None
    else:
        dst_width = width
        dst_height = height
    write_display_raster(
        file_path=file_path,
        display_file_path=display_file_path,
        raw_profile=raw_profile,
        dst_width=dst_width,
        dst_height=dst_height,
        overview_levels=overview_levels,
        colormap=raster_info.colormap,
        resampling=resampling,
    )

    result = Artifact(
        **metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.RASTER_MAP_LAYER,
        filename=file_path.name,
        attachments=Attachments(legend=legend, display_filename=display_file_path.name),
    )

    log.debug(f'Returning Artifact: {result.model_dump()}.')

    return result


def write_raw_raster(
    data_array: MaskedArray,
    file_path: Path,
    raw_profile: dict,
    indexes: int | list[int],
    colormap: Optional[Colormap],
    overview_levels: list[int],
    resampling: str,
) -> None:
    with rasterio.open(file_path, mode='w', **raw_profile) as out_map_file:
        if colormap:
            out_map_file.write_colormap(1, colormap)

        out_map_file.write(data_array, indexes=indexes)

        out_map_file.build_overviews(overview_levels, OverviewResampling[resampling])
        out_map_file.update_tags(ns='rio_overview', resampling=resampling)


def write_display_raster(
    file_path: Path,
    display_file_path: Path,
    raw_profile: dict,
    dst_width: Optional[int],
    dst_height: Optional[int],
    overview_levels: list[int],
    colormap: Optional[Colormap],
    resampling: str,
) -> None:
    dst_crs = CRS({'init': 'epsg:3857'})
    with rasterio.open(file_path, mode='r') as src:
        left, bottom, right, top = src.bounds
        display_transform, display_width, display_height = calculate_default_transform(
            src_crs=src.crs,
            dst_crs=dst_crs,
            width=src.width,
            height=src.height,
            left=left,
            bottom=bottom,
            right=right,
            top=top,
            dst_width=dst_width,
            dst_height=dst_height,
        )
        display_profile = deepcopy(raw_profile)
        display_profile.update(
            {'crs': dst_crs, 'transform': display_transform, 'width': display_width, 'height': display_height}
        )

        with rasterio.open(display_file_path, 'w', **display_profile) as dst:
            if colormap:
                dst.write_colormap(1, colormap)

            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=display_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling[resampling],
                )

            dst.build_overviews(overview_levels, OverviewResampling[resampling])
            dst.update_tags(ns='rio_overview', resampling=resampling)


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
