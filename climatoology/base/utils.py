from typing import Callable

import geojson_pydantic
import shapely
from pandas import DataFrame
from shapely import set_srid


def deep_apply_dataframe(
    df: DataFrame,
    func: Callable,
    *,
    apply_index_name: bool = True,
    apply_index_values: bool = True,
    exclude_columns: tuple[str] = tuple(),
    exclude_column_names: tuple[str] = tuple(),
) -> DataFrame:
    """
    Deep apply `func` to the provided dataframe.

    :param df: the dataframe.
    :param func: the function to apply. Can be any callable, but we do no checking if the datatype or similar are fitting.
    :param apply_index_name: whether to apply the func to the index name (MultiIndex not supported). Defaults
      to True.
    :param apply_index_values: whether to apply the func to the values of the index (MultiIndex not supported).
      Defaults to True.
    :param exclude_columns: columns for which the values should not be considered. By default, all columns will be
      considered.
    :param exclude_column_names: column names which should not be considered. By default, all column names will be
      considered.
    :return: a copy of the dataframe after applying func.
    """
    result_df = df.copy(deep=True)

    if apply_index_name:
        result_df.index = result_df.index.rename(func(result_df.index.name))

    if apply_index_values:
        result_df.index = result_df.index.map(func)

    apply_col_names = list(set(df.columns) - set(exclude_columns))
    result_df[apply_col_names] = result_df[apply_col_names].map(func)

    result_df.columns = [func(c) if c not in exclude_column_names else c for c in result_df.columns]

    return result_df


def deep_apply_dict(data: dict, func: Callable, target_keys: set) -> dict:
    result_dict = dict()
    for k, v in data.items():
        if isinstance(v, dict):
            new_value = deep_apply_dict(data=v, func=func, target_keys=target_keys)
        elif k in target_keys:
            new_value = func(v)
        else:
            new_value = v
        result_dict[k] = new_value

    return result_dict


def shapely_from_geojson_pydantic(geojson_geom: geojson_pydantic.geometries.Geometry) -> shapely.Geometry:
    shapely_geom = shapely.geometry.shape(context=geojson_geom)
    shapely_geom = set_srid(geometry=shapely_geom, srid=4326)
    return shapely_geom
