import pandas as pd
from pandas.testing import assert_frame_equal

from climatoology.base.i18n import N_, translate_dataframe


def test_translate_dataframe(set_to_german):
    input_df = pd.DataFrame(
        index=pd.Index([N_('index_1'), N_('index_2')], name=N_('index_name')),
        columns=[N_('column_1'), N_('column_2')],
        data=[
            [N_('value_1'), 1],
            [N_('value_2'), 2],
        ],
    )

    expected_df = pd.DataFrame(
        index=pd.Index(['de_index_1', 'de_index_2'], name='de_index_name'),
        columns=['de_column_1', 'de_column_2'],
        data=[
            ['de_value_1', 1],
            ['de_value_2', 2],
        ],
    )

    translated_df = translate_dataframe(input_df)

    assert_frame_equal(translated_df, expected_df)


def test_translate_dataframe_exclude_column_name(set_to_german):
    input_df = pd.DataFrame(
        index=pd.Index([N_('index_1'), N_('index_2')], name=N_('index_name')),
        columns=[N_('column_1'), N_('column_2')],
        data=[
            [N_('value_1'), 1],
            [N_('value_2'), 2],
        ],
    )

    expected_df = pd.DataFrame(
        index=pd.Index(['de_index_1', 'de_index_2'], name='de_index_name'),
        columns=['de_column_1', 'column_2'],
        data=[
            ['de_value_1', 1],
            ['de_value_2', 2],
        ],
    )

    translated_df = translate_dataframe(input_df, exclude_column_names=('column_2'))

    assert_frame_equal(translated_df, expected_df)
