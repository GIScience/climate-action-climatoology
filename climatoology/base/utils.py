from typing import Callable

from pandas import DataFrame


def deep_apply_dataframe(
    df: DataFrame,
    func: Callable,
    *,
    apply_index_name: bool = True,
    apply_index_values: bool = True,
    exclude_columns: tuple[str] = (),
    exclude_column_names: tuple[str] = (),
) -> DataFrame:
    """
    Deep apply `func` to the provided dataframe.

    :param df: the dataframe.
    :param apply_index_name: whether or not to apply the func to the index name (MultiIndex not supported). Defaults
      to True.
    :param apply_index_values: whether or not to apply the func to the values of the index (MultiIndex not supported).
      Defaults to True.
    :param exclude_columns: columns for which the values should not be considered. By default all columns will be
      considered.
    :param exclude_column_names: column names which should not be considered. By default all column names will be
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
